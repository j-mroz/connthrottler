package iorate

import (
	"bytes"
	"fmt"
	"io"
	"net"
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

type readWriteTestBenchConfig struct {
	bps         ByteSize
	burst       ByteSize
	payloadSize ByteSize
	wantErr     bool
	r           io.Reader
	w           io.Writer
}

type readWriteTestResult struct {
	throughput float64
}

var readerWriterTests = []readWriteTestBenchConfig{
	{bps: 1 * KiB, burst: 512 * B, payloadSize: 4 * KiB, wantErr: false},
	{bps: 250 * KiB, burst: 32 * KiB, payloadSize: 1 * MiB, wantErr: false},
	{bps: 1 * MiB, burst: 32 * KiB, payloadSize: 10 * MiB, wantErr: false},
}

func TestWriteLimiter_Write_InIoCopy(t *testing.T) {
	var writeSink bytes.Buffer

	for _, tt := range readerWriterTests {
		testName := fmt.Sprintf("payload-%s__limit-%s__burst-%s", tt.payloadSize, tt.bps, tt.burst)
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
	var writeSink bytes.Buffer

	for _, tt := range readerWriterTests {
		testName := fmt.Sprintf("payload-%s__limit-%s__burst-%s", tt.payloadSize, tt.bps, tt.burst)
		payload := make([]byte, tt.payloadSize)

		t.Run(testName, func(t *testing.T) {
			readSource := bytes.NewReader(payload)
			writeSink.Grow(int(tt.payloadSize))
			writeSink.Reset()
			tt.r = NewReadLimiter(readSource, tt.bps, tt.burst)
			tt.w = &writeSink
			readWriteTest(t, &tt)
		})
	}
}

func readWriteTest(t *testing.T, args *readWriteTestBenchConfig) {
	copyStartTime := time.Now()
	written, err := io.Copy(args.w, args.r)
	copyTime := time.Since(copyStartTime)

	if (err != nil) != args.wantErr {
		t.Errorf("io.Copy error = %v, wantErr %v", err, args.wantErr)
		return
	}
	if written != int64(args.payloadSize) {
		t.Errorf("io.Copy write = %v, want %v", written, args.payloadSize)
	}

	throughput := float64(written) / copyTime.Seconds()
	minBps := float64(args.bps) * 0.95
	maxBps := float64(args.bps) * 1.05
	if !(minBps <= throughput) || !(throughput <= maxBps) {
		t.Errorf("throughput = %f, accepted range [%f, %f]", throughput, minBps, maxBps)
	}
}

// Benchamarks for determining optimal optimal bps and burst ratio
var readWriteBenchmarks = []readWriteTestBenchConfig{
	{bps: 1 * KiB, burst: 512 * B, payloadSize: 4 * KiB},
	{bps: 1 * KiB, burst: 1 * KiB, payloadSize: 4 * KiB},

	{bps: 250 * KiB, burst: 1 * KiB, payloadSize: 4 * MiB},
	{bps: 250 * KiB, burst: 32 * KiB, payloadSize: 4 * MiB},
	{bps: 250 * KiB, burst: 250 * KiB, payloadSize: 4 * MiB},

	{bps: 1 * MiB, burst: 1 * KiB, payloadSize: 4 * MiB},
	{bps: 1 * MiB, burst: 32 * KiB, payloadSize: 4 * MiB},
	{bps: 1 * MiB, burst: 250 * KiB, payloadSize: 4 * MiB},
	{bps: 1 * MiB, burst: 1 * MiB, payloadSize: 4 * MiB},

	// 	{bps: 250 * MiB, burst: 30 * KiB, payloadSize: 1 * GiB},
	// 	{bps: 250 * MiB, burst: 250 * KiB, payloadSize: 1 * GiB},
	// 	{bps: 250 * MiB, burst: 1 * MiB, payloadSize: 1 * GiB},
	// 	{bps: 250 * MiB, burst: 5 * MiB, payloadSize: 1 * GiB},
	// 	{bps: 250 * MiB, burst: 1 * MiB, payloadSize: 1 * GiB},
	// 	{bps: 250 * MiB, burst: 5 * MiB, payloadSize: 1 * GiB},
	// 	{bps: 250 * MiB, burst: 25 * MiB, payloadSize: 1 * GiB},
}

func Benchmark_Reader(b *testing.B) {
	var writeSink bytes.Buffer

	for _, bm := range readWriteBenchmarks {
		testName := fmt.Sprintf("payload-%s__limit-%s__burst-%s", bm.payloadSize, bm.bps, bm.burst)

		writeSink.Grow(int(bm.payloadSize))
		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			b.ReportAllocs()

			var avgTroughput float64

			for runN := 0; runN < b.N; runN++ {
				writeSink.Reset()
				readLimiter := NewReadLimiter(bytes.NewReader(payload), bm.bps, bm.burst)
				avgTroughput += readWriteBenchmark(b, readLimiter, &writeSink)
			}

			avgTroughput /= float64(b.N)
			b.ReportMetric(avgTroughput, "B/s")
		})
	}
}

func Benchmark_Writer(b *testing.B) {
	var writeSink bytes.Buffer

	for _, bm := range readWriteBenchmarks {
		testName := fmt.Sprintf("payload-%s__limit-%s__burst-%s", bm.payloadSize, bm.bps, bm.burst)

		writeSink.Grow(int(bm.payloadSize))
		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			b.ReportAllocs()

			var avgTroughput float64

			for runN := 0; runN < b.N; runN++ {
				writeSink.Reset()
				payloadReader := bytes.NewReader(payload)
				writeLimiter := NewWriteLimiter(&writeSink, bm.bps, bm.burst)
				avgTroughput += readWriteBenchmark(b, payloadReader, writeLimiter)
			}

			avgTroughput /= float64(b.N)
			b.ReportMetric(avgTroughput, "B/s")
		})
	}
}

func readWriteBenchmark(b *testing.B, r io.Reader, w io.Writer) (throughput float64) {
	copyStartTime := time.Now()
	written, _ := io.Copy(w, r)
	copyTime := time.Since(copyStartTime)
	return float64(written) / copyTime.Seconds()
}

func TestListenerLimiter_Accept(t *testing.T) {
	l := &MockListener{}
	listenerLimiter := NewListenerLimiter(l)
	conn, _ := listenerLimiter.Accept()

	_, connStored := listenerLimiter.connections.Load(conn)
	if !connStored {
		t.Errorf("ListenerLimiter.Accept() should store accepted connections")
	}
}

func TestListenerLimiter_SetBandwithLimits(t *testing.T) {
	l := &MockListener{}
	listenerLimiter := NewListenerLimiter(l)

	bpsPerListenerLimit := ByteSize(100 * KiB)
	bpsPerConnLimit := ByteSize(2 * KiB)

	listenerLimiter.SetBandwithLimits(bpsPerListenerLimit, bpsPerConnLimit)

	if bpsPerListenerLimit != listenerLimiter.bpsPerListenerLimit ||
		bpsPerConnLimit != listenerLimiter.bpsPerConnLimit {
		t.Errorf("ListenerLimiter.SetBandwithLimits limits sets incorrectly")
	}

	//TODO: add connection limits when connection is implemented
}

type MockListener struct {
	addr net.Addr
}

func (l *MockListener) Accept() (net.Conn, error) {
	return nil, nil
}

func (l *MockListener) Close() error {
	return nil
}

func (l *MockListener) Addr() net.Addr {
	return l.addr
}
