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
//   - If byteStart > 0, the first (possibly partial) line is skipped.
//   - Lines are read until the reader has consumed past byteEnd.
//
// When byteEnd == 0 the entire object is read with no boundary trimming.
func readSplitRecords(ctx context.Context, storage objectStorage, dataURI string, byteStart, byteEnd int64, checksum string) ([][]byte, error) {
	bucket, key, err := parseS3URI(dataURI)
	if err != nil {
		return nil, err
	}

	var raw []byte
	if byteEnd == 0 {
		rc, err := storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("GetObject %s/%s: %w", bucket, key, err)
		}
		defer rc.Close()
		raw, err = io.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		byteEnd = int64(len(raw)) - 1
		if byteEnd < 0 {
			return nil, nil // empty object
		}
	} else {
		raw, err = getRawRange(ctx, storage, bucket, key, byteStart, byteEnd)
		if err != nil {
			return nil, err
		}
		// Validate SHA-256 of exactly the claimed [byteStart, byteEnd] bytes.
		if checksum != "" {
			rangeLen := byteEnd - byteStart + 1
			if int64(len(raw)) >= rangeLen {
				if err := validateChecksum(raw[:rangeLen], checksum); err != nil {
					return nil, fmt.Errorf("split checksum: %w", err)
				}
			}
		}
	}

	return extractSplitLines(raw, byteStart, byteEnd)
}

// extractSplitLines applies JSONL boundary rules to raw bytes that begin at
// byteStart in the original file. byteEnd is an absolute offset in the original file.
//
// Rules:
//   - If byteStart > 0, discard the first line (it's a partial record from the previous split).
//   - Emit lines until the reader position in the original file exceeds byteEnd.
//   - The line that crosses byteEnd is always emitted in full (finish-the-record rule).
func extractSplitLines(raw []byte, byteStart, byteEnd int64) ([][]byte, error) {
	br := bufio.NewReaderSize(bytes.NewReader(raw), shuffle.DefaultMaxRecordBytes)

	var records [][]byte
	var offset int64 // bytes consumed from raw so far

	// If byteStart > 0 we may be mid-line; skip until the first newline.
	if byteStart > 0 {
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
