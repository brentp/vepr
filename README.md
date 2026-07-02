# vepr

`vepr` runs Ensembl VEP on large multi-sample VCFs without making VEP carry the sample columns.

This exists because VEP can become very memory hungry and much slower when a VCF has thousands of samples. Most VEP annotations depend on the variant columns, not every genotype field. `vepr` strips sample fields before annotation, runs VEP on smaller chunks in parallel, then restores the original `FORMAT` and sample columns to the annotated records.

## What it does

1. Runs `bcftools view -I` once and splits each record into its eight site columns (fed to VEP) and a sidecar holding the original `FORMAT` and sample columns.
2. Splits the site stream into chunks. The default chunk size is `500000` variants.
3. Runs multiple VEP processes. The default is `2` concurrent processes.
4. Buffers out-of-order finished chunks and writes them back in the original order.
5. Pastes the original `FORMAT` and sample columns back onto the matching annotated variants, verified by `CHROM/POS/REF/ALT` so a reordered or dropped VEP record is rejected rather than misaligned.
6. Sends the final stream through `bcftools view` so output can be plain VCF, bgzipped VCF, or BCF.

## Requirements

- Go 1.22 or newer to build.
- `bcftools` available on `PATH`, or passed with `--bcftools-bin`.
- `vep` available on `PATH`, or passed with `--vep-bin`.

## Build

```bash
go build -o vepr .
```

## Usage

Put `vepr` options before `--`. Put VEP options after `--`; they are passed through to each VEP process.

```bash
vepr \
  --input cohort.vcf.gz \
  --output cohort.vep.vcf.gz \
  --chunk-size 500000 \
  --jobs 2 \
  -- \
  --cache \
  --offline \
  --assembly GRCh38 \
  --species homo_sapiens
```

## Options

- `--input`, `-i`: input VCF/BCF. Required.
- `--output`, `-o`: output path ending in `.vcf`, `.vcf.gz`, `.vcf.bgz`, or `.bcf`. Defaults to plain VCF on stdout.
- `--chunk-size`: variants per VEP chunk. Defaults to `500000`.
- `--jobs`, `-j`: parallel VEP processes. Defaults to `2`.
- `--bcftools-bin`: bcftools executable. Defaults to `bcftools`.
- `--vep-bin`: VEP executable. Defaults to `vep`.
- `--tmp-dir`: parent directory for temporary chunk files.
- `--keep-temp`: keep temporary chunk files for debugging.
- `--vep-stats`: allow VEP stats files. By default `vepr` adds `--no_stats` for chunk runs.

## Notes

The output format is selected from `--output`:

- `*.vcf`: plain VCF via `bcftools view -Ov`
- `*.vcf.gz` or `*.vcf.bgz`: bgzipped VCF via `bcftools view -Oz`
- `*.bcf`: BCF via `bcftools view -Ob`
- `-`: plain VCF to stdout

The output order matches the input order. If chunks finish out of order, `vepr` buffers completed chunks until the next expected chunk is ready to write.

Sample paste-back validates `CHROM`, `POS`, `REF`, and `ALT`; VEP options that rewrite coordinates or alleles will fail rather than risk attaching samples to the wrong variant.

Use `vepr`'s `--jobs` for parallelism, not VEP's own `--fork`. Paste-back matches each annotated record to its samples in order, so if `--fork` reorders records within a chunk the run will abort with a sample key mismatch.

Index compressed output with `bcftools index`:

```bash
bcftools index -t cohort.vep.vcf.gz
bcftools index cohort.vep.bcf
```
