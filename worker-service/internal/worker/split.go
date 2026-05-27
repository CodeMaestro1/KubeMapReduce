package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"

	"kubemapreduce/worker-service/internal/shuffle"
)

// readSplitRecords downloads a split from MinIO, validates the SHA-256 checksum
// on the claimed byte range, then extracts JSONL records with boundary alignment:
//
//   - If byteStart > 0 and the byte immediately before byteStart is not '\n',
//     the first (partial) line is skipped because it belongs to the previous split.
//   - If byteStart > 0 and the preceding byte IS '\n', byteStart falls exactly on
//     a line boundary and the first record is retained in full.
//   - Lines are read until the reader has consumed past byteEnd.
//
// When byteEnd == 0 the entire object is read with no boundary trimming.
// readSplitRecordsStreaming returns an io.Reader that streams JSONL records from
// MinIO with boundary alignment. It avoids buffering the entire split in RAM.
func readSplitRecordsStreaming(ctx context.Context, storage objectStorage, dataURI string, byteStart, byteEnd int64, checksum string) (io.ReadCloser, error) {
	bucket, key, err := parseS3URI(dataURI)
	if err != nil {
		return nil, err
	}

	if byteEnd == 0 {
		rc, err := storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("GetObject %s/%s: %w", bucket, key, err)
		}
		return rc, nil
	}

	// Download the exact byte range for checksum validation and line processing.
	rawRange, err := getRawRange(ctx, storage, bucket, key, byteStart, byteEnd)
	if err != nil {
		return nil, err
	}

	// Validate SHA-256 of exactly the claimed [byteStart, byteEnd] bytes.
	if checksum != "" {
		rangeLen := byteEnd - byteStart + 1
		if int64(len(rawRange)) >= rangeLen {
			if err := validateChecksum(rawRange[:rangeLen], checksum); err != nil {
				return nil, fmt.Errorf("split checksum: %w", err)
			}
		}
	}

	// Determine boundary alignment.
	atBoundary := true
	if byteStart > 0 {
		atBoundary, err = fetchPrecedingByte(ctx, storage, bucket, key, byteStart)
		if err != nil {
			return nil, fmt.Errorf("checking split boundary at offset %d: %w", byteStart, err)
		}
	}

	// Stream lines from the validated buffer via a pipe.
	pr, pw := io.Pipe()
	go func() {
		br := bufio.NewReaderSize(bytes.NewReader(rawRange), shuffle.DefaultMaxRecordBytes)
		var offset int64

		if byteStart > 0 && !atBoundary {
			skipped, err := br.ReadBytes('\n')
			if err != nil {
				pw.CloseWithError(fmt.Errorf("skip partial line: %w", err))
				return
			}
			offset += int64(len(skipped))
		}

		for {
			line, err := br.ReadBytes('\n')
			if err != nil && err != io.EOF {
				pw.CloseWithError(fmt.Errorf("read line: %w", err))
				return
			}
			if len(line) > 0 {
				if _, writeErr := pw.Write(line); writeErr != nil {
					pw.CloseWithError(writeErr)
					return
				}
				offset += int64(len(line))
			}
			if err == io.EOF {
				break
			}
			if byteStart+offset > byteEnd {
				break
			}
		}
		pw.Close()
	}()

	return pr, nil
}

// extractSplitLines applies JSONL boundary rules to raw bytes that begin at
// byteStart in the original file. byteEnd is an absolute offset in the original file.
// atLineBoundary must be true when byteStart == 0 or when the byte immediately
// before byteStart in the original file is '\n' (i.e. byteStart is exactly on a
// record boundary).
//
// Rules:
//   - If byteStart > 0 and !atLineBoundary, discard the first line (partial record
//     belonging to the previous split).
//   - If byteStart > 0 and atLineBoundary, retain the first line (it is a complete
//     record that starts exactly at this split boundary).
//   - Emit lines until the reader position in the original file exceeds byteEnd.
//   - The line that crosses byteEnd is always emitted in full (finish-the-record rule).
func extractSplitLines(raw []byte, byteStart, byteEnd int64, atLineBoundary bool) ([][]byte, error) {
	br := bufio.NewReaderSize(bytes.NewReader(raw), shuffle.DefaultMaxRecordBytes)

	var records [][]byte
	var offset int64 // bytes consumed from raw so far

	// If byteStart > 0 and not on a line boundary, we are mid-line; skip until
	// the first newline so we don't emit a partial JSONL record.
	if byteStart > 0 && !atLineBoundary {
		skipped, err := br.ReadBytes('\n')
		if err == io.EOF {
			return nil, fmt.Errorf("incomplete JSONL record before split start")
		}
		if err != nil {
			return nil, fmt.Errorf("skip partial line: %w", err)
		}
		offset += int64(len(skipped))
	}

	for {
		line, err := br.ReadBytes('\n')
		atEOF := err == io.EOF
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read line: %w", err)
		}

		if len(line) > 0 {
			trimmed := bytes.TrimRight(line, "\r\n")
			if len(trimmed) > 0 {
				records = append(records, append([]byte(nil), trimmed...))
			}
			offset += int64(len(line))
		}

		if atEOF {
			break
		}

		// Stop after emitting the line that crossed byteEnd (finish-the-record).
		if byteStart+offset > byteEnd {
			break
		}
	}
	return records, nil
}
