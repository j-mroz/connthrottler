package iorate

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestBytesUnit_String(t *testing.T) {
	tests := []struct {
		name string
		b    ByteSize
		want string
	}{
		{"1B", 1 * B, "1.00B"},
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

type readWriteTestArgs struct {
	bps         ByteSize
	burst       ByteSize
	payloadSize ByteSize
	wantErr     bool
	bpsAssert   bool
	r           io.Reader
	w           io.Writer
}

type readWriteTestResult struct {
	throughput float64
}

func TestWriteLimiter_Write_InIoCopy(t *testing.T) {
	tests := []readWriteTestArgs{
		{bps: 1 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},
		{bps: 1 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
		{bps: 1 * MiB, burst: 512 * B, payloadSize: 10 * MiB, wantErr: false},
		{bps: 1 * MiB, burst: 2 * KiB, payloadSize: 10 * MiB, wantErr: false},
	}

	var writeSink bytes.Buffer

	for _, tt := range tests {
		testName := fmt.Sprintf("payload-%s__limit-%s__burst-%s", tt.payloadSize, tt.bps, tt.burst)
		tt.bpsAssert = true

		payload := make([]byte, tt.payloadSize)
		writeSink.Grow(int(tt.payloadSize))
		writeSink.Reset()

		t.Run(testName, func(t *testing.T) {
			tt.w = NewWriteLimiter(&writeSink, tt.bps, tt.burst)
			tt.r = bytes.NewReader(payload)
			readWriteTest(t, &tt)

		})
	}
}

func TestWriteLimiter_Read_InIoCopy(t *testing.T) {
	tests := []readWriteTestArgs{
		{bps: 1 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},
		{bps: 1 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
	}
	for _, tt := range tests {
		testName := fmt.Sprintf("limit-%s__burst-%s", tt.bps, tt.burst)
		tt.bpsAssert = true

		payload := make([]byte, tt.payloadSize)
		var writeSink bytes.Buffer
		writeSink.Grow(int(tt.payloadSize))

		t.Run(testName, func(t *testing.T) {
			readSource := bytes.NewReader(payload)
			writeSink.Reset()

			tt.r = NewReadLimiter(readSource, tt.bps, tt.burst)
			tt.w = &writeSink
			readWriteTest(t, &tt)
		})
	}
}

func Benchmark_Reader(b *testing.B) {
	benchmarks := []readWriteTestArgs{
		{bps: 1 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},
		{bps: 1 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
	}

	for _, bm := range benchmarks {
		testName := fmt.Sprintf("payload-%s__limit-%s__burst-%s", bm.payloadSize, bm.bps, bm.burst)
		var writeSink bytes.Buffer
		writeSink.Grow(int(bm.payloadSize))
		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			var avgTroughput float64

			for runN := 0; runN < b.N; runN++ {
				readSource := bytes.NewReader(payload)
				writeSink.Reset()

				bm.r = NewReadLimiter(readSource, bm.bps, bm.burst)
				bm.w = &writeSink

				testResult := readWriteTest(b, &bm)
				avgTroughput += testResult.throughput
			}

			avgTroughput /= float64(b.N)

			b.ReportMetric(avgTroughput, "B/s")
		})
	}
}

func Benchmark_Writer(b *testing.B) {
	benchmarks := []readWriteTestArgs{
		{bps: 1 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 2 * KiB, wantErr: false},

		{bps: 1 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
		{bps: 4 * KiB, burst: 512 * B, payloadSize: 10 * KiB, wantErr: false},
		{bps: 1 * MiB, burst: 2 * KiB, payloadSize: 10 * MiB, wantErr: false},

		{bps: 250 * MiB, burst: 30 * KiB, payloadSize: 1 * GiB, wantErr: false},
		{bps: 250 * MiB, burst: 250 * KiB, payloadSize: 1 * GiB, wantErr: false},
		{bps: 250 * MiB, burst: 1 * MiB, payloadSize: 1 * GiB, wantErr: false},
		{bps: 250 * MiB, burst: 5 * MiB, payloadSize: 1 * GiB, wantErr: false},

		{bps: 250 * MiB, burst: 1 * MiB, payloadSize: 1 * GiB, wantErr: false},
		{bps: 250 * MiB, burst: 5 * MiB, payloadSize: 1 * GiB, wantErr: false},
		{bps: 250 * MiB, burst: 25 * MiB, payloadSize: 1 * GiB, wantErr: false},
	}

	for _, bm := range benchmarks {
		bm.bpsAssert = false
		testName := fmt.Sprintf("payload-%s__limit-%s__burst-%s", bm.payloadSize, bm.bps, bm.burst)

		var writeSink bytes.Buffer
		writeSink.Grow(int(bm.payloadSize))
		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			var avgTroughput float64

			for runN := 0; runN < b.N; runN++ {
				writeSink.Reset()
				bm.r = bytes.NewReader(payload)
				bm.w = NewWriteLimiter(&writeSink, bm.bps, bm.burst)

				testResult := readWriteTest(b, &bm)
				avgTroughput += testResult.throughput
			}

			avgTroughput /= float64(b.N)

			b.ReportMetric(avgTroughput, "B/s")
		})
	}
}

func readWriteTest(tb testing.TB, args *readWriteTestArgs) (ret readWriteTestResult) {
	copyStartTime := time.Now()
	written, err := io.Copy(args.w, args.r)
	copyTime := time.Since(copyStartTime)

	if (err != nil) != args.wantErr {
		tb.Errorf("io.Copy error = %v, wantErr %v", err, args.wantErr)
		return
	}
	if written != int64(args.payloadSize) {
		tb.Errorf("io.Copy write = %v, want %v", written, args.payloadSize)
	}

	throughput := float64(written) / copyTime.Seconds()
	ret.throughput = throughput

	if args.bpsAssert {
		minBps := float64(args.bps) * 0.95
		maxBps := float64(args.bps) * 1.05
		if !(minBps <= throughput) || !(throughput <= maxBps) {
			tb.Errorf("throughput = %f, accepted range [%f, %f]", throughput, minBps, maxBps)
		}
	}

	return
}
