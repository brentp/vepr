package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/spf13/pflag"
)

const defaultChunkSize = 10000

type config struct {
	Input       string
	Output      string
	ChunkSize   int
	Jobs        int
	VEPBin      string
	BCFToolsBin string
	TmpDir      string
	KeepTemp    bool
	VEPStats    bool
	LogLevel    string
}

type headerMeta struct {
	chunkHeader       []string
	originalChromLine string
}

type chunkJob struct {
	index      int
	inputPath  string
	samplePath string
}

type chunkResult struct {
	chunkJob
	outputPath string
	err        error
}

type processReader struct {
	name     string
	cmd      *exec.Cmd
	stderr   *bytes.Buffer
	reader   *lineReader
	waitOnce sync.Once
	waitErr  error
}

type lineReader struct {
	r *bufio.Reader
}

func main() {
	cfg, vepArgs, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	if err := run(ctx, cfg, vepArgs); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "interrupted")
			os.Exit(130)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	stop()
}

func parseArgs(args []string) (config, []string, error) {
	cfg := config{
		Output:      "-",
		ChunkSize:   defaultChunkSize,
		Jobs:        2,
		VEPBin:      "vep",
		BCFToolsBin: "bcftools",
		LogLevel:    "info",
	}

	flags := pflag.NewFlagSet("vepr", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVarP(&cfg.Input, "input", "i", cfg.Input, "input VCF/BCF path")
	flags.StringVarP(&cfg.Output, "output", "o", cfg.Output, "output .vcf, .vcf.gz, .vcf.bgz, or .bcf path; use - for stdout")
	flags.IntVar(&cfg.ChunkSize, "chunk-size", cfg.ChunkSize, "variants per VEP chunk")
	flags.IntVarP(&cfg.Jobs, "jobs", "j", cfg.Jobs, "parallel VEP processes")
	flags.StringVar(&cfg.VEPBin, "vep-bin", cfg.VEPBin, "VEP executable")
	flags.StringVar(&cfg.BCFToolsBin, "bcftools-bin", cfg.BCFToolsBin, "bcftools executable")
	flags.StringVar(&cfg.TmpDir, "tmp-dir", cfg.TmpDir, "temporary directory")
	flags.BoolVar(&cfg.KeepTemp, "keep-temp", cfg.KeepTemp, "keep temporary chunk files")
	flags.BoolVar(&cfg.VEPStats, "vep-stats", cfg.VEPStats, "allow VEP to write per-chunk stats files")
	flags.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level: debug, info, warn, or error")

	if err := flags.Parse(args); err != nil {
		return config{}, nil, usageError(err)
	}
	if cfg.Input == "" {
		return config{}, nil, usageError(errors.New("--input is required"))
	}
	if cfg.ChunkSize <= 0 {
		return config{}, nil, usageError(errors.New("--chunk-size must be greater than zero"))
	}
	if cfg.Jobs <= 0 {
		return config{}, nil, usageError(errors.New("--jobs must be greater than zero"))
	}
	if _, err := parseLogLevel(cfg.LogLevel); err != nil {
		return config{}, nil, usageError(err)
	}
	vepArgs := flags.Args()
	if len(vepArgs) > 0 && vepArgs[0] == "--" {
		vepArgs = vepArgs[1:]
	}
	return cfg, vepArgs, nil
}

func usageError(err error) error {
	return fmt.Errorf("%w\n\nusage: vepr --input in.vcf.gz --output out.vcf.gz --chunk-size 10000 --jobs 2 -- [vep args]", err)
}

func run(ctx context.Context, cfg config, vepArgs []string) error {
	logger, err := newLogger(cfg.LogLevel)
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp(cfg.TmpDir, "vepr-")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	if !cfg.KeepTemp {
		defer os.RemoveAll(tmpDir)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	out, closeOut, err := outputWriter(ctx, cfg)
	if err != nil {
		return err
	}
	outputClosed := false
	defer func() {
		if !outputClosed {
			_ = closeOut()
		}
	}()

	jobs := make(chan chunkJob, cfg.Jobs*2)
	results := make(chan chunkResult, cfg.Jobs*2)
	metaCh := make(chan headerMeta, 1)
	producerErr := make(chan error, 1)

	go func() {
		producerErr <- produceChunks(ctx, logger, cfg, tmpDir, metaCh, jobs)
		close(jobs)
	}()

	var meta headerMeta
	select {
	case meta = <-metaCh:
	case err := <-producerErr:
		cancel()
		if err != nil {
			return err
		}
		select {
		case meta = <-metaCh:
		default:
			return errors.New("input contained no VCF header")
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	startWorkers(ctx, logger, cfg, tmpDir, vepArgs, jobs, results)
	runErr := consumeResults(ctx, cancel, out, meta, results, producerErr, cfg.KeepTemp)
	closeErr := closeOut()
	outputClosed = true
	if runErr != nil {
		return runErr
	}
	if closeErr != nil {
		return fmt.Errorf("close output: %w", closeErr)
	}
	return nil
}

func newLogger(levelName string) (*slog.Logger, error) {
	level, err := parseLogLevel(levelName)
	if err != nil {
		return nil, err
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})), nil
}

func parseLogLevel(name string) (slog.Level, error) {
	if name == "" {
		name = "info"
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(name))); err != nil {
		return slog.LevelInfo, fmt.Errorf("unsupported --log-level %q; use debug, info, warn, or error", name)
	}
	return level, nil
}

func startWorkers(ctx context.Context, logger *slog.Logger, cfg config, tmpDir string, vepArgs []string, jobs <-chan chunkJob, results chan<- chunkResult) {
	var workers sync.WaitGroup
	for i := 0; i < cfg.Jobs; i++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			for job := range jobs {
				logger.InfoContext(ctx, "starting VEP chunk", "worker", workerID, "chunk", job.index)
				res := runVEPChunk(ctx, cfg, tmpDir, vepArgs, job)
				select {
				case results <- res:
					logger.DebugContext(ctx, "sent VEP chunk result", "channel", "results", "chunk", res.index, "error", res.err != nil)
				case <-ctx.Done():
					return
				}
			}
		}(i + 1)
	}
	go func() {
		workers.Wait()
		close(results)
	}()
}

func outputWriter(ctx context.Context, cfg config) (*bufio.Writer, func() error, error) {
	if cfg.Output == "-" {
		w := bufio.NewWriter(os.Stdout)
		return w, w.Flush, nil
	}

	outputType, err := bcftoolsOutputType(cfg.Output)
	if err != nil {
		return nil, nil, err
	}

	cmd := interruptibleCommand(ctx, cfg.BCFToolsBin, "view", outputType, "-o", cfg.Output, "-")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("open bcftools output stdin: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("start bcftools output writer: %w", err)
	}

	w := bufio.NewWriter(stdin)
	var closeOnce sync.Once
	var closeErr error
	closeFn := func() error {
		closeOnce.Do(func() {
			flushErr := w.Flush()
			stdinErr := stdin.Close()
			waitErr := cmd.Wait()
			switch {
			case flushErr != nil:
				closeErr = flushErr
			case stdinErr != nil:
				closeErr = stdinErr
			case waitErr != nil:
				msg := strings.TrimSpace(stderr.String())
				if msg == "" {
					closeErr = fmt.Errorf("bcftools output writer failed: %w", waitErr)
				} else {
					closeErr = fmt.Errorf("bcftools output writer failed: %w: %s", waitErr, msg)
				}
			}
		})
		return closeErr
	}
	return w, closeFn, nil
}

func bcftoolsOutputType(path string) (string, error) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".bcf"):
		return "-Ob", nil
	case strings.HasSuffix(lower, ".vcf.gz"), strings.HasSuffix(lower, ".vcf.bgz"):
		return "-Oz", nil
	case strings.HasSuffix(lower, ".vcf"):
		return "-Ov", nil
	default:
		return "", fmt.Errorf("unsupported output extension for %s; use .vcf, .vcf.gz, .vcf.bgz, .bcf, or -", path)
	}
}

func produceChunks(ctx context.Context, logger *slog.Logger, cfg config, tmpDir string, metaCh chan<- headerMeta, jobs chan<- chunkJob) error {
	bcftoolsCtx, cancel := context.WithCancel(ctx)

	full, err := startBCFTools(bcftoolsCtx, cfg.BCFToolsBin, "view", []string{"view", "-I", "--threads", "3", "-O", "v", cfg.Input})
	if err != nil {
		cancel()
		return err
	}
	defer func() {
		cancel()
		_ = full.wait()
	}()

	fullHeader, originalChromLine, next, err := readVCFHeader(full.reader)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("read VCF header: %w", err)
	}
	if originalChromLine == "" {
		if next == nil {
			if err := full.wait(); err != nil {
				return err
			}
		}
		return errors.New("VCF is missing #CHROM header")
	}
	chunkHeader := sampleFreeHeader(fullHeader)

	select {
	case metaCh <- headerMeta{chunkHeader: chunkHeader, originalChromLine: originalChromLine}:
		logger.DebugContext(ctx, "sent VCF header metadata", "channel", "metaCh", "header_lines", len(chunkHeader))
	case <-ctx.Done():
		return ctx.Err()
	}

	chunkIndex := 0
	rowsInChunk := 0
	var current *chunkFiles

	closeCurrent := func() error {
		if current == nil {
			return nil
		}
		job, err := current.close()
		if err != nil {
			return err
		}
		select {
		case jobs <- job:
			logger.DebugContext(ctx, "sent VEP chunk job", "channel", "jobs", "chunk", job.index)
		case <-ctx.Done():
			return ctx.Err()
		}
		current = nil
		return nil
	}

	for next != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if current == nil {
			current, err = newChunkFiles(tmpDir, chunkIndex, chunkHeader)
			if err != nil {
				return err
			}
			rowsInChunk = 0
		}
		siteLine, sampleRecord, err := splitRecord(*next)
		if err != nil {
			return fmt.Errorf("record %d: %w", chunkIndex*cfg.ChunkSize+rowsInChunk+1, err)
		}
		if err := current.writeRecord(siteLine, sampleRecord); err != nil {
			return err
		}
		rowsInChunk++
		if rowsInChunk == cfg.ChunkSize {
			if err := closeCurrent(); err != nil {
				return err
			}
			chunkIndex++
		}

		next, err = full.reader.readLine()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("read VCF record: %w", err)
		}
	}
	if err := closeCurrent(); err != nil {
		return err
	}
	return full.wait()
}

// sampleFreeHeader returns the header with the #CHROM line truncated to the
// eight fixed VCF site columns, dropping FORMAT and sample names.
func sampleFreeHeader(header []string) []string {
	out := make([]string, len(header))
	for i, line := range header {
		if strings.HasPrefix(line, "#CHROM\t") {
			out[i] = siteColumns(line)
		} else {
			out[i] = line
		}
	}
	return out
}

func siteColumns(line string) string {
	fields := strings.SplitN(line, "\t", 9)
	if len(fields) <= 8 {
		return line
	}
	return strings.Join(fields[:8], "\t")
}

type chunkFiles struct {
	job        chunkJob
	inputFile  *os.File
	tailFile   *os.File
	writer     *bufio.Writer
	tailWriter *bufio.Writer
}

func newChunkFiles(tmpDir string, index int, header []string) (*chunkFiles, error) {
	inputPath := filepath.Join(tmpDir, "chunk-"+strconv.Itoa(index)+".vcf")
	samplePath := filepath.Join(tmpDir, "chunk-"+strconv.Itoa(index)+".samples")

	inputFile, err := os.Create(inputPath)
	if err != nil {
		return nil, fmt.Errorf("create chunk input: %w", err)
	}
	tailFile, err := os.Create(samplePath)
	if err != nil {
		inputFile.Close()
		return nil, fmt.Errorf("create sample tail file: %w", err)
	}

	c := &chunkFiles{
		job:        chunkJob{index: index, inputPath: inputPath, samplePath: samplePath},
		inputFile:  inputFile,
		tailFile:   tailFile,
		writer:     bufio.NewWriter(inputFile),
		tailWriter: bufio.NewWriter(tailFile),
	}
	for _, line := range header {
		if _, err := fmt.Fprintln(c.writer, line); err != nil {
			c.closeFiles()
			return nil, fmt.Errorf("write chunk header: %w", err)
		}
	}
	return c, nil
}

func (c *chunkFiles) writeRecord(vcfLine, sampleTail string) error {
	if _, err := fmt.Fprintln(c.writer, vcfLine); err != nil {
		return fmt.Errorf("write chunk record: %w", err)
	}
	if _, err := fmt.Fprintln(c.tailWriter, sampleTail); err != nil {
		return fmt.Errorf("write sample tail: %w", err)
	}
	return nil
}

func (c *chunkFiles) close() (chunkJob, error) {
	if err := c.writer.Flush(); err != nil {
		c.closeFiles()
		return chunkJob{}, fmt.Errorf("flush chunk input: %w", err)
	}
	if err := c.tailWriter.Flush(); err != nil {
		c.closeFiles()
		return chunkJob{}, fmt.Errorf("flush sample tails: %w", err)
	}
	if err := c.inputFile.Close(); err != nil {
		c.tailFile.Close()
		return chunkJob{}, fmt.Errorf("close chunk input: %w", err)
	}
	if err := c.tailFile.Close(); err != nil {
		return chunkJob{}, fmt.Errorf("close sample tails: %w", err)
	}
	return c.job, nil
}

func (c *chunkFiles) closeFiles() {
	_ = c.inputFile.Close()
	_ = c.tailFile.Close()
}

func startBCFTools(ctx context.Context, bin, name string, args []string) (*processReader, error) {
	cmd := interruptibleCommand(ctx, bin, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("start bcftools %s: %w", name, err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start bcftools %s: %w", name, err)
	}
	return &processReader{
		name:   "bcftools " + name,
		cmd:    cmd,
		stderr: &stderr,
		reader: &lineReader{r: bufio.NewReader(stdout)},
	}, nil
}

func (p *processReader) wait() error {
	p.waitOnce.Do(func() {
		err := p.cmd.Wait()
		if err == nil {
			return
		}
		msg := strings.TrimSpace(p.stderr.String())
		if msg == "" {
			p.waitErr = fmt.Errorf("%s failed: %w", p.name, err)
			return
		}
		p.waitErr = fmt.Errorf("%s failed: %w: %s", p.name, err, msg)
	})
	return p.waitErr
}

func readVCFHeader(r *lineReader) ([]string, string, *string, error) {
	var header []string
	var chromLine string
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, "", nil, err
		}
		if line == nil {
			return header, chromLine, nil, nil
		}
		if strings.HasPrefix(*line, "#") {
			header = append(header, *line)
			if strings.HasPrefix(*line, "#CHROM\t") {
				chromLine = *line
			}
			continue
		}
		return header, chromLine, line, nil
	}
}

func (r *lineReader) readLine() (*string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			if line == "" {
				return nil, nil
			}
		} else {
			return nil, err
		}
	}
	line = strings.TrimRight(line, "\r\n")
	return &line, nil
}

// splitRecord separates a full VCF record into the site line fed to VEP (the
// eight fixed columns) and a sidecar record that carries the variant key plus
// the original FORMAT and sample columns for pasting back after annotation.
func splitRecord(fullLine string) (siteLine, sampleRecord string, err error) {
	fields := strings.SplitN(fullLine, "\t", 10)
	if len(fields) < 8 {
		return "", "", errors.New("VCF record has fewer than 8 columns")
	}
	siteLine = strings.Join(fields[:8], "\t")
	if len(fields) <= 8 {
		return siteLine, variantKey(fields) + "\t", nil
	}
	return siteLine, variantKey(fields) + "\t" + strings.Join(fields[8:], "\t"), nil
}

func variantKey(fields []string) string {
	return strings.Join([]string{fields[0], fields[1], fields[3], fields[4]}, "\t")
}

func runVEPChunk(ctx context.Context, cfg config, tmpDir string, vepArgs []string, job chunkJob) chunkResult {
	outputPath := filepath.Join(tmpDir, "chunk-"+strconv.Itoa(job.index)+".vep.vcf")
	args := make([]string, 0, len(vepArgs)+8)
	args = append(args, vepArgs...)
	if !cfg.VEPStats {
		args = append(args, "--no_stats")
	}
	args = append(args,
		"--vcf",
		"--force_overwrite",
		"--input_file", job.inputPath,
		"--output_file", outputPath,
	)

	cmd := interruptibleCommand(ctx, cfg.VEPBin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return chunkResult{chunkJob: job, err: ctxErr}
		}
		return chunkResult{chunkJob: job, err: fmt.Errorf("vep chunk %d failed: %w", job.index, err)}
	}
	return chunkResult{chunkJob: job, outputPath: outputPath}
}

func consumeResults(ctx context.Context, cancel context.CancelFunc, out *bufio.Writer, meta headerMeta, results <-chan chunkResult, producerErr <-chan error, keepTemp bool) error {
	pending := map[int]chunkResult{}
	next := 0
	headerWritten := false
	var firstErr error
	producerDone := false
	ctxDone := ctx.Done()

	flushReady := func() {
		for firstErr == nil {
			res, ok := pending[next]
			if !ok {
				return
			}
			delete(pending, next)
			if err := writeAnnotatedChunk(out, meta.originalChromLine, &headerWritten, res); err != nil {
				firstErr = err
				cancel()
				return
			}
			if !keepTemp {
				if err := removeChunkFiles(res); err != nil {
					firstErr = err
					cancel()
					return
				}
			}
			next++
		}
	}

	for results != nil || !producerDone {
		select {
		case err := <-producerErr:
			producerDone = true
			producerErr = nil
			if err != nil && firstErr == nil {
				firstErr = err
				cancel()
			}
		case res, ok := <-results:
			if !ok {
				results = nil
				continue
			}
			if res.err != nil {
				if firstErr == nil {
					firstErr = res.err
					cancel()
				}
				continue
			}
			pending[res.index] = res
			flushReady()
		case <-ctxDone:
			if firstErr == nil {
				firstErr = ctx.Err()
				cancel()
			}
			ctxDone = nil
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if !headerWritten {
		for _, line := range meta.chunkHeader {
			if strings.HasPrefix(line, "#CHROM\t") {
				line = meta.originalChromLine
			}
			if _, err := fmt.Fprintln(out, line); err != nil {
				return err
			}
		}
	}
	return out.Flush()
}

func writeAnnotatedChunk(out *bufio.Writer, originalChromLine string, headerWritten *bool, res chunkResult) error {
	vepFile, err := os.Open(res.outputPath)
	if err != nil {
		return fmt.Errorf("open VEP output chunk %d: %w", res.index, err)
	}
	defer vepFile.Close()
	sampleFile, err := os.Open(res.samplePath)
	if err != nil {
		return fmt.Errorf("open sample tails chunk %d: %w", res.index, err)
	}
	defer sampleFile.Close()

	vepReader := &lineReader{r: bufio.NewReader(vepFile)}
	sampleReader := &lineReader{r: bufio.NewReader(sampleFile)}
	for {
		line, err := vepReader.readLine()
		if err != nil {
			return fmt.Errorf("read VEP output chunk %d: %w", res.index, err)
		}
		if line == nil {
			break
		}
		if strings.HasPrefix(*line, "#") {
			if !*headerWritten {
				outLine := *line
				if strings.HasPrefix(outLine, "#CHROM\t") {
					outLine = originalChromLine
					*headerWritten = true
				}
				if _, err := fmt.Fprintln(out, outLine); err != nil {
					return err
				}
			}
			continue
		}
		tail, err := sampleReader.readLine()
		if err != nil {
			return fmt.Errorf("read sample tail chunk %d: %w", res.index, err)
		}
		if tail == nil {
			return fmt.Errorf("sample tails ended early for chunk %d", res.index)
		}
		outLine, err := pasteSampleTail(*line, *tail)
		if err != nil {
			return fmt.Errorf("paste sample tail for chunk %d: %w", res.index, err)
		}
		if _, err := fmt.Fprintln(out, outLine); err != nil {
			return err
		}
	}
	extraTail, err := sampleReader.readLine()
	if err != nil {
		return fmt.Errorf("read sample tail chunk %d: %w", res.index, err)
	}
	if extraTail != nil {
		return fmt.Errorf("sample tails remain after VEP output ended for chunk %d", res.index)
	}
	return nil
}

func removeChunkFiles(res chunkResult) error {
	for _, path := range []string{res.inputPath, res.samplePath, res.outputPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove temporary chunk file %s: %w", path, err)
		}
	}
	return nil
}

func pasteSampleTail(annotatedLine, sampleRecord string) (string, error) {
	sampleFields := strings.SplitN(sampleRecord, "\t", 5)
	if len(sampleFields) < 4 {
		return "", errors.New("sample sidecar record has fewer than 4 key columns")
	}
	if len(sampleFields) == 4 {
		sampleFields = append(sampleFields, "")
	}

	sampleKey := strings.Join(sampleFields[:4], "\t")
	sampleTail := sampleFields[4]
	fields := strings.SplitN(annotatedLine, "\t", 9)
	if len(fields) < 8 {
		return "", errors.New("annotated VCF record has fewer than 8 columns")
	}
	annotatedKey := variantKey(fields)
	if annotatedKey != sampleKey {
		return "", fmt.Errorf("sample key mismatch: annotated %s vs samples %s", annotatedKey, sampleKey)
	}
	if sampleTail == "" {
		return annotatedLine, nil
	}
	return strings.Join(fields[:8], "\t") + "\t" + sampleTail, nil
}
