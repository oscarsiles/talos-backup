// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package compression provides methods for compressing backups.
package compression

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"github.com/siderolabs/talos-backup/pkg/util"
)

// CompressFile compresses a file with gzip.
func CompressFile(fileToCompressPath string) (string, error) {
	compressedFileName, err := compressFile(fileToCompressPath)

	if err != nil && compressedFileName != "" {
		util.CleanupFile(compressedFileName)
	}

	return compressedFileName, err
}

// compressFile compresses a file with gzip.
func compressFile(fileToCompressPath string) (string, error) {
	fileToCompress, err := os.OpenFile(fileToCompressPath, os.O_RDONLY, 0o600)
	if err != nil {
		return "", fmt.Errorf("failed to open file for compression %q: %w", fileToCompressPath, err)
	}

	defer fileToCompress.Close() //nolint:errcheck

	compressedFileName := fileToCompressPath + ".gz"

	compressedFile, err := os.OpenFile(compressedFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", fmt.Errorf("failed to allocate compressed file %q: %w", compressedFileName, err)
	}

	defer compressedFile.Close() //nolint:errcheck

	w, err := gzip.NewWriterLevel(compressedFile, gzip.BestCompression)
	if err != nil {
		return "", fmt.Errorf("failed to compress file %q: %w", fileToCompressPath, err)
	}

	if _, err := io.Copy(w, fileToCompress); err != nil {
		return "", fmt.Errorf("failed to write compressed file %q: %w", compressedFileName, err)
	}

	if err := w.Close(); err != nil {
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	if err := compressedFile.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync compressed file to disk: %w", err)
	}

	return compressedFileName, nil
}
