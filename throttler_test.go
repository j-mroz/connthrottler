package connthrottler

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestBytesUnit_String(t *testing.T) {
	tests := []struct {
		name string
		b    BytesUnit
		want string
	}{
		{"1B", BytesUnit(1), "1.00B"},
		{"1KiB", 1 * KiB, "1.00KiB"},
		{"1GiB", 1 * MiB, "1.00MiB"},
		{"1GiB", 1 * GiB, "1.00GiB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.b.String(); got != tt.want {
				t.Errorf("BytesUnit.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteLimiter_Write_Copy(t *testing.T) {
	payload := make([]byte, 10*KiB)

	tests := []struct {
		bps           BytesUnit
		burst         BytesUnit
		wantWriteSize int
		wantErr       bool
	}{
		{bps: 1 * KiB, burst: 1 * KiB, wantWriteSize: len(payload), wantErr: false},
	}

	for _, tt := range tests {
		testName := fmt.Sprintf("limit-%s__burst-%s", tt.bps, tt.burst)
		t.Run(testName, func(t *testing.T) {
			var writeSink bytes.Buffer

			wl := NewWriteLimiter(&writeSink, tt.bps, tt.burst)

			written, err := io.Copy(wl, bytes.NewReader(payload))

			if (err != nil) != tt.wantErr {
				t.Errorf("io.Copy error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if written != int64(tt.wantWriteSize) {
				t.Errorf("io.Copy write = %v, want %v", written, tt.wantWriteSize)
			}
		})
	}
}

func TestWriteLimiter_Read_InIoCopy(t *testing.T) {
	payload := make([]byte, 10*KiB)

	tests := []struct {
		bps           BytesUnit
		burst         BytesUnit
		wantWriteSize int
		wantErr       bool
	}{
		{bps: 1 * KiB, burst: 1 * KiB, wantWriteSize: len(payload), wantErr: false},
	}

	for _, tt := range tests {
		testName := fmt.Sprintf("limit-%s__burst-%s", tt.bps, tt.burst)
		t.Run(testName, func(t *testing.T) {
			readSource := bytes.NewReader(payload)
			readLimiter := NewReadLimiter(readSource, tt.bps, tt.burst)

			var writeSink bytes.Buffer
			written, err := io.Copy(&writeSink, readLimiter)

			if (err != nil) != tt.wantErr {
				t.Errorf("io.Copy error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if written != int64(tt.wantWriteSize) {
				t.Errorf("io.Copy write = %v, want %v", written, tt.wantWriteSize)
			}
		})
	}
}
