package p2put

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

type CompressMode int

const (
	CompressAuto CompressMode = iota
	CompressOn
	CompressOff
)

var compressMode = CompressAuto
var sampleSize = 65536
var magicThreshold = 0.85

type magicEntry struct {
	name  string
	magic []byte
}

var magicHeaders = []magicEntry{
	{"gzip", []byte{0x1F, 0x8B}},
	{"zstd", []byte{0x28, 0xB5, 0x2F, 0xFD}},
	{"lz4", []byte{0x04, 0x22, 0x4D, 0x18}},
	{"xz", []byte{0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00}},
	{"bzip2", []byte{0x42, 0x5A, 0x68}},
	{"7z", []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}},
	{"png", []byte{0x89, 0x50, 0x4E, 0x47}},
	{"jpeg", []byte{0xFF, 0xD8, 0xFF}},
	{"webp", []byte{0x52, 0x49, 0x46, 0x46}},
	{"gif", []byte{0x47, 0x49, 0x46, 0x38}},
	{"zip", []byte{0x50, 0x4B, 0x03, 0x04}},
	{"rar", []byte{0x52, 0x61, 0x72, 0x21, 0x1A, 0x07}},
	{"mp3", []byte{0xFF, 0xFB}},
	{"mp4", []byte{0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70}},
	{"avi", []byte{0x52, 0x49, 0x46, 0x46}},
}

var compressedExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true,
	".mp4": true, ".avi": true, ".mkv": true, ".mov": true,
	".mp3": true, ".aac": true, ".flac": true, ".ogg": true, ".wav": true,
	".zip": true, ".gz": true, ".zst": true, ".bz2": true, ".xz": true,
	".7z": true, ".rar": true,
}

func SetCompressMode(m CompressMode) {
	compressMode = m
}

func detectCompression(path string) string {
	mode := compressMode
	if mode == CompressOn {
		return "zstd"
	}
	if mode == CompressOff {
		return "none"
	}

	ext := strings.ToLower(filepath.Ext(path))
	if compressedExts[ext] {
		return "none"
	}

	sample := readSample(path, sampleSize)
	if len(sample) == 0 {
		return "none"
	}

	if hasMagicHeader(sample) {
		return "none"
	}

	entropy := calcEntropy(sample)
	if entropy >= 7.5 {
		return "none"
	}
	if entropy <= 6.0 {
		return "zstd"
	}

	ratio := compressionRatio(sample)
	if ratio < magicThreshold {
		return "zstd"
	}
	return "none"
}

func readSample(path string, size int) []byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	buf := make([]byte, size)
	n, _ := io.ReadFull(f, buf)
	return buf[:n]
}

func hasMagicHeader(sample []byte) bool {
	for _, e := range magicHeaders {
		if len(sample) >= len(e.magic) {
			match := true
			for i, b := range e.magic {
				if sample[i] != b {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func calcEntropy(sample []byte) float64 {
	if len(sample) == 0 {
		return 0
	}
	var freq [256]int
	for _, b := range sample {
		freq[b]++
	}
	n := float64(len(sample))
	var h float64
	log2 := math.Log2
	for _, c := range freq {
		if c > 0 {
			p := float64(c) / n
			h -= p * log2(p)
		}
	}
	return h
}

func compressionRatio(sample []byte) float64 {
	compressed := zstdCompressBlock(sample)
	if len(compressed) == 0 {
		return 1.0
	}
	return float64(len(compressed)) / float64(len(sample))
}

func zstdCompressBlock(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil
	}
	w.Write(src)
	w.Close()
	return buf.Bytes()
}

func zstdDecompressBlock(src []byte, uncompLen int) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	r, err := zstd.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	dst := make([]byte, uncompLen)
	if _, err := io.ReadFull(r, dst); err != nil {
		return nil, err
	}
	return dst, nil
}

func packDataFrame(seq int, data []byte, compression string) []byte {
	if compression == "zstd" {
		compressed := zstdCompressBlock(data)
		pay := make([]byte, 8+4+len(compressed))
		binary.BigEndian.PutUint64(pay[:8], uint64(seq))
		binary.BigEndian.PutUint32(pay[8:12], uint32(len(data)))
		copy(pay[12:], compressed)
		return pay
	}
	pay := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(pay[:8], uint64(seq))
	copy(pay[8:], data)
	return pay
}

func unpackDataFrame(payload []byte, compression string) (seq int64, data []byte, err error) {
	if len(payload) < 8 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	seq = int64(binary.BigEndian.Uint64(payload[:8]))
	if compression == "zstd" {
		if len(payload) < 12 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		uncompLen := int(binary.BigEndian.Uint32(payload[8:12]))
		data, err = zstdDecompressBlock(payload[12:], uncompLen)
		return seq, data, err
	}
	return seq, payload[8:], nil
}
