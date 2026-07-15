package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunAnnotatesChunksAndRestoresSamples(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.vcf")
	output := filepath.Join(dir, "output.vcf")

	inputVCF := strings.Join([]string{
		"##fileformat=VCFv4.2",
		"##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
		"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ts1\ts2",
		"1\t10\t.\tA\tC\t.\tPASS\t.\tGT\t0/1\t0/0",
		"1\t20\t.\tG\tT\t.\tPASS\t.\tGT\t1/1\t0/1",
		"1\t30\t.\tC\tA\t.\tPASS\t.\tGT\t0/0\t1/1",
		"",
	}, "\n")
	if err := os.WriteFile(input, []byte(inputVCF), 0o644); err != nil {
		t.Fatal(err)
	}

	bcftools := filepath.Join(dir, "bcftools")
	if err := os.WriteFile(bcftools, []byte(fakeBCFToolsScript), 0o755); err != nil {
		t.Fatal(err)
	}
	vep := filepath.Join(dir, "vep")
	if err := os.WriteFile(vep, []byte(fakeVEPScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := config{
		Input:       input,
		Output:      output,
		ChunkSize:   2,
		Jobs:        2,
		VEPBin:      vep,
		BCFToolsBin: bcftools,
		TmpDir:      dir,
	}
	if err := run(context.Background(), cfg, []string{"--cache"}); err != nil {
		t.Fatal(err)
	}

	gotBytes, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	got := string(gotBytes)
	if !strings.Contains(got, "##INFO=<ID=CSQ") {
		t.Fatalf("missing VEP INFO header:\n%s", got)
	}
	if !strings.Contains(got, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ts1\ts2") {
		t.Fatalf("missing original sample header:\n%s", got)
	}

	lines := dataLines(got)
	want := []string{
		"1\t10\t.\tA\tC\t.\tPASS\tCSQ=fake\tGT\t0/1\t0/0",
		"1\t20\t.\tG\tT\t.\tPASS\tCSQ=fake\tGT\t1/1\t0/1",
		"1\t30\t.\tC\tA\t.\tPASS\tCSQ=fake\tGT\t0/0\t1/1",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("records mismatch\ngot:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

func TestRunKeepsOutputOrderWhenChunksFinishOutOfOrder(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.vcf")
	output := filepath.Join(dir, "output.vcf")

	inputVCF := strings.Join([]string{
		"##fileformat=VCFv4.2",
		"##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
		"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ts1",
		"1\t10\t.\tA\tC\t.\tPASS\t.\tGT\t0/1",
		"1\t20\t.\tG\tT\t.\tPASS\t.\tGT\t1/1",
		"1\t30\t.\tC\tA\t.\tPASS\t.\tGT\t0/0",
		"",
	}, "\n")
	if err := os.WriteFile(input, []byte(inputVCF), 0o644); err != nil {
		t.Fatal(err)
	}

	bcftools := filepath.Join(dir, "bcftools")
	if err := os.WriteFile(bcftools, []byte(fakeBCFToolsScript), 0o755); err != nil {
		t.Fatal(err)
	}
	vep := filepath.Join(dir, "vep")
	if err := os.WriteFile(vep, []byte(fakeSlowFirstChunkVEPScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := config{
		Input:       input,
		Output:      output,
		ChunkSize:   1,
		Jobs:        2,
		VEPBin:      vep,
		BCFToolsBin: bcftools,
		TmpDir:      dir,
	}
	if err := run(context.Background(), cfg, nil); err != nil {
		t.Fatal(err)
	}

	gotBytes, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	lines := dataLines(string(gotBytes))
	want := []string{
		"1\t10\t.\tA\tC\t.\tPASS\tCSQ=fake\tGT\t0/1",
		"1\t20\t.\tG\tT\t.\tPASS\tCSQ=fake\tGT\t1/1",
		"1\t30\t.\tC\tA\t.\tPASS\tCSQ=fake\tGT\t0/0",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("records mismatch\ngot:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

func TestRunReturnsOnProducerErrorAfterHeader(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.vcf")
	output := filepath.Join(dir, "output.vcf")

	if err := os.WriteFile(input, []byte("unused\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bcftools := filepath.Join(dir, "bcftools")
	if err := os.WriteFile(bcftools, []byte(fakeTruncatedRecordBCFToolsScript), 0o755); err != nil {
		t.Fatal(err)
	}
	vep := filepath.Join(dir, "vep")
	if err := os.WriteFile(vep, []byte(fakeVEPScript), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg := config{
		Input:       input,
		Output:      output,
		ChunkSize:   1,
		Jobs:        2,
		VEPBin:      vep,
		BCFToolsBin: bcftools,
		TmpDir:      dir,
	}
	err := run(ctx, cfg, nil)
	if err == nil {
		t.Fatal("expected producer error")
	}
	if !strings.Contains(err.Error(), "fewer than 8 columns") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunHeaderOnlyInput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.vcf")
	output := filepath.Join(dir, "output.vcf")

	inputVCF := strings.Join([]string{
		"##fileformat=VCFv4.2",
		"##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
		"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ts1",
		"",
	}, "\n")
	if err := os.WriteFile(input, []byte(inputVCF), 0o644); err != nil {
		t.Fatal(err)
	}
	bcftools := filepath.Join(dir, "bcftools")
	if err := os.WriteFile(bcftools, []byte(fakeBCFToolsScript), 0o755); err != nil {
		t.Fatal(err)
	}
	vep := filepath.Join(dir, "vep")
	if err := os.WriteFile(vep, []byte(fakeVEPScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := config{
		Input:       input,
		Output:      output,
		ChunkSize:   1,
		Jobs:        2,
		VEPBin:      vep,
		BCFToolsBin: bcftools,
		TmpDir:      dir,
	}
	if err := run(context.Background(), cfg, nil); err != nil {
		t.Fatal(err)
	}

	gotBytes, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	got := string(gotBytes)
	if !strings.Contains(got, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ts1") {
		t.Fatalf("missing sample header:\n%s", got)
	}
	if lines := dataLines(got); len(lines) != 0 {
		t.Fatalf("expected no data lines, got %v", lines)
	}
}

func TestRunRejectsReorderedVEPRecords(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.vcf")
	output := filepath.Join(dir, "output.vcf")

	inputVCF := strings.Join([]string{
		"##fileformat=VCFv4.2",
		"##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
		"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\ts1",
		"1\t10\t.\tA\tC\t.\tPASS\t.\tGT\t0/1",
		"1\t20\t.\tG\tT\t.\tPASS\t.\tGT\t1/1",
		"",
	}, "\n")
	if err := os.WriteFile(input, []byte(inputVCF), 0o644); err != nil {
		t.Fatal(err)
	}
	bcftools := filepath.Join(dir, "bcftools")
	if err := os.WriteFile(bcftools, []byte(fakeBCFToolsScript), 0o755); err != nil {
		t.Fatal(err)
	}
	vep := filepath.Join(dir, "vep")
	if err := os.WriteFile(vep, []byte(fakeReorderingVEPScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := config{
		Input:       input,
		Output:      output,
		ChunkSize:   2,
		Jobs:        1,
		VEPBin:      vep,
		BCFToolsBin: bcftools,
		TmpDir:      dir,
	}
	err := run(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("expected reordered VEP output to fail")
	}
	if !strings.Contains(err.Error(), "sample key mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunVEPChunkStreamsProcessOutput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.vcf")
	if err := os.WriteFile(input, []byte("##fileformat=VCFv4.2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	vep := filepath.Join(dir, "vep")
	if err := os.WriteFile(vep, []byte(fakeLoggingVEPScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := config{VEPBin: vep}
	var res chunkResult
	stdout, stderr := captureStdFiles(t, func() {
		res = runVEPChunk(context.Background(), cfg, dir, nil, chunkJob{index: 1, inputPath: input})
	})
	if res.err != nil {
		t.Fatal(res.err)
	}
	if stdout != "vep stdout\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	if stderr != "vep stderr\n" {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestRunReportsBCFToolsFailureBeforeMissingHeader(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.vcf")
	output := filepath.Join(dir, "output.vcf")
	if err := os.WriteFile(input, []byte("unused\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bcftools := filepath.Join(dir, "bcftools")
	if err := os.WriteFile(bcftools, []byte(fakeFailingBCFToolsScript), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := config{
		Input:       input,
		Output:      output,
		ChunkSize:   1,
		Jobs:        1,
		VEPBin:      filepath.Join(dir, "vep"),
		BCFToolsBin: bcftools,
		TmpDir:      dir,
	}
	err := run(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("expected bcftools failure")
	}
	if strings.Contains(err.Error(), "VCF is missing #CHROM header") {
		t.Fatalf("bcftools failure was masked: %v", err)
	}
	if !strings.Contains(err.Error(), "could not open input") {
		t.Fatalf("missing bcftools stderr: %v", err)
	}
}

func TestBCFToolsOutputTypeFromFilename(t *testing.T) {
	tests := map[string]string{
		"out.vcf":     "-Ov",
		"out.vcf.gz":  "-Oz",
		"out.vcf.bgz": "-Oz",
		"out.bcf":     "-Ob",
	}
	for name, want := range tests {
		got, err := bcftoolsOutputType(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != want {
			t.Fatalf("%s: got %s, want %s", name, got, want)
		}
	}

	if _, err := bcftoolsOutputType("out.txt"); err == nil {
		t.Fatal("expected unsupported extension error")
	}
}

func TestParseArgsDropsSeparatorBeforeVEPArgs(t *testing.T) {
	_, vepArgs, err := parseArgs([]string{"--input", "in.vcf.gz", "--", "--cache", "--offline"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--cache", "--offline"}
	if strings.Join(vepArgs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got %v, want %v", vepArgs, want)
	}
}

func TestParseArgsDropsExtraSeparatorBeforeVEPArgs(t *testing.T) {
	_, vepArgs, err := parseArgs([]string{"--input", "in.vcf.gz", "--", "--", "--cache", "--offline"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--cache", "--offline"}
	if strings.Join(vepArgs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got %v, want %v", vepArgs, want)
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"":      slog.LevelInfo,
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for name, want := range tests {
		got, err := parseLogLevel(name)
		if err != nil {
			t.Fatalf("%q: %v", name, err)
		}
		if got != want {
			t.Fatalf("%q: got %v, want %v", name, got, want)
		}
	}

	if _, err := parseLogLevel("trace"); err == nil {
		t.Fatal("expected unsupported log level error")
	}
}

func dataLines(vcf string) []string {
	var out []string
	for _, line := range strings.Split(vcf, "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

func captureStdFiles(t *testing.T, fn func()) (string, string) {
	t.Helper()
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		_ = stderrR.Close()
		_ = stderrW.Close()
	}()

	os.Stdout = stdoutW
	os.Stderr = stderrW
	fn()

	if err := stdoutW.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stderrW.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	stdout, err := io.ReadAll(stdoutR)
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := io.ReadAll(stderrR)
	if err != nil {
		t.Fatal(err)
	}
	return string(stdout), string(stderr)
}

const fakeBCFToolsScript = `#!/bin/sh
drop=0
out=
input=
for arg in "$@"; do
  if [ "$skip_next" = "1" ]; then
    skip_next=
    continue
  fi
  case "$arg" in
    view|-I|-Ov|-Oz|-Ob) ;;
    -G) drop=1 ;;
    -O|--threads) skip_next=1 ;;
    -o) want_out=1 ;;
    *)
      if [ "$want_out" = "1" ]; then
        out="$arg"
        want_out=
      else
        input="$arg"
      fi
      ;;
  esac
done
if [ -z "$input" ] || [ "$input" = "-" ]; then
  input=/dev/stdin
fi
if [ -n "$out" ]; then
  exec > "$out"
fi
awk -v drop="$drop" 'BEGIN{FS=OFS="\t"}
  /^##/ { print; next }
  /^#CHROM/ {
    if (drop) { print $1,$2,$3,$4,$5,$6,$7,$8 } else { print }
    next
  }
  {
    if (drop) { print $1,$2,$3,$4,$5,$6,$7,$8 } else { print }
  }' "$input"
`

const fakeFailingBCFToolsScript = `#!/bin/sh
printf '%s\n' 'could not open input' >&2
exit 2
`

const fakeVEPScript = `#!/bin/sh
in=
out=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --input_file) shift; in="$1" ;;
    --output_file) shift; out="$1" ;;
  esac
  shift
done
awk 'BEGIN{FS=OFS="\t"; added=0}
  /^##/ { print; next }
  /^#CHROM/ {
    if (!added) { print "##INFO=<ID=CSQ,Number=.,Type=String,Description=\"fake\">"; added=1 }
    print
    next
  }
  {
    if ($8 == ".") { $8 = "CSQ=fake" } else { $8 = $8 ";CSQ=fake" }
    print
  }' "$in" > "$out"
`

const fakeSlowFirstChunkVEPScript = `#!/bin/sh
in=
out=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --input_file) shift; in="$1" ;;
    --output_file) shift; out="$1" ;;
  esac
  shift
done
if awk 'BEGIN{FS="\t"; found=0} !/^#/ && $2 == "10" { found=1 } END{ exit found ? 0 : 1 }' "$in"; then
  sleep 0.2
fi
awk 'BEGIN{FS=OFS="\t"; added=0}
  /^##/ { print; next }
  /^#CHROM/ {
    if (!added) { print "##INFO=<ID=CSQ,Number=.,Type=String,Description=\"fake\">"; added=1 }
    print
    next
  }
  {
    if ($8 == ".") { $8 = "CSQ=fake" } else { $8 = $8 ";CSQ=fake" }
    print
  }' "$in" > "$out"
`

const fakeReorderingVEPScript = `#!/bin/sh
in=
out=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --input_file) shift; in="$1" ;;
    --output_file) shift; out="$1" ;;
  esac
  shift
done
awk 'BEGIN{FS=OFS="\t"; added=0}
  /^##/ { print; next }
  /^#CHROM/ {
    if (!added) { print "##INFO=<ID=CSQ,Number=.,Type=String,Description=\"fake\">"; added=1 }
    print
    next
  }
  {
    if ($8 == ".") { $8 = "CSQ=fake" } else { $8 = $8 ";CSQ=fake" }
    records[++n] = $0
  }
  END {
    for (i = n; i >= 1; i--) {
      print records[i]
    }
  }' "$in" > "$out"
`

const fakeLoggingVEPScript = `#!/bin/sh
out=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output_file) shift; out="$1" ;;
  esac
  shift
done
printf '%s\n' 'vep stdout'
printf '%s\n' 'vep stderr' >&2
: > "$out"
`

// fakeTruncatedRecordBCFToolsScript emits a valid header followed by a record
// with fewer than eight columns, so the producer fails only after the header
// has already been handed off to the consumer.
const fakeTruncatedRecordBCFToolsScript = `#!/bin/sh
out=
for arg in "$@"; do
  if [ "$want_out" = "1" ]; then out="$arg"; want_out=; fi
  if [ "$arg" = "-o" ]; then want_out=1; fi
done
if [ -n "$out" ]; then
  cat > "$out"
  exit 0
fi
printf '%s\n' '##fileformat=VCFv4.2'
printf '%s\n' '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	s1'
printf '%s\n' '1	10	.	A	C'
`
