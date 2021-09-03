package iorate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
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

func TestReader_Read_SharedLimit(t *testing.T) {
	payload := []byte("0123456789")
	bps := 2 * B
	expectReadSize := 2
	limiter := NewBandwidthLimiter(bps)
	limitedReader := NewReaderWithSharedLimit(bytes.NewReader(payload), limiter)

	buff := make([]byte, 32*1024)
	n, err := limitedReader.Read(buff)

	if n != expectReadSize {
		t.Errorf("Reader.Read() = %v, want %v", n, expectReadSize)
	}
	if err != nil {
		t.Errorf("Reader.Read() error = %v", err)
	}
}

func TestReader_Read_ChainedReaders(t *testing.T) {
	payload := []byte("0123456789")
	expectRead := 3

	reader1 := NewReader(bytes.NewReader(payload), 5*B)
	reader2 := NewReader(reader1, 3*B)
	reader3 := NewReader(reader2, 10*B)

	buff := make([]byte, 32*1024)
	read, err := reader3.Read(buff)

	assertEqual(t, read, expectRead,
		"Reader.Read() = %v, want %v", read, expectRead)
	if err != nil {
		t.Errorf("Reader.Read() error = %v", err)
	}
}

func TestWriter_Write(t *testing.T) {
	type given struct {
		payload string
		bps     ByteSize
	}
	type expect struct {
		fullWrite bool
		err       error
	}
	tests := []struct {
		given  given
		expect expect
	}{
		{
			given{payload: "", bps: 0 * KiB},
			expect{fullWrite: false, err: ErrNoBurstAvilable},
		}, {
			given{payload: "", bps: 1 * KiB},
			expect{fullWrite: true, err: nil},
		}, {
			given{payload: "01234567890", bps: 1 * B},
			expect{fullWrite: false, err: nil},
		}, {
			given{payload: "01234567890", bps: 1 * KiB},
			expect{fullWrite: true, err: nil},
		},
	}

	for _, tt := range tests {
		testName := fmt.Sprintf("payload:<%s>_limit:%s",
			tt.given.payload, tt.given.bps)

		t.Run(testName, func(t *testing.T) {
			var writeSink bytes.Buffer
			limitedWriter := NewWriter(&writeSink, tt.given.bps)

			payload := []byte(tt.given.payload)
			n, err := limitedWriter.Write([]byte(payload))
			expectN := len(payload)
			if n != expectN {
				t.Errorf("Writer.Write() = %v, want %v", n, expectN)
			}
			if !errors.Is(err, tt.expect.err) {
				t.Errorf("Writer.Write() error = %v, want err %v",
					err, tt.expect.err)
			}
		})
	}
}

type readWriteTestBenchConfig struct {
	bps         ByteSize
	payloadSize ByteSize
	chain       ByteSize // enabled if not 0
	wantErr     bool
	r           io.Reader
	w           io.Writer
}

// var readerWriterTests = []readWriteTestBenchConfig{
// 	{bps: 1 * KiB, payloadSize: 4 * KiB, wantErr: false},
// 	{bps: 1 * KiB, chain: 1 * KiB, payloadSize: 4 * KiB, wantErr: false},
// 	{bps: 1 * KiB, chain: 800 * B, payloadSize: 4 * KiB, wantErr: false},
// 	{bps: 2 * KiB, payloadSize: 4 * KiB, wantErr: false},
// 	{bps: 250 * KiB, payloadSize: 1 * MiB, wantErr: false},
// 	{bps: 250 * KiB, chain: 200 * KiB, payloadSize: 1 * MiB, wantErr: false},

// 	{bps: 1 * MiB, payloadSize: 4 * MiB, wantErr: false},
// }

// func TestWriter_Write_InIoCopy(t *testing.T) {

// 	for _, tt := range readerWriterTests {

// 		testName := fmt.Sprintf("payload:%s_limit:%s", tt.payloadSize, tt.bps)

// 		t.Run(testName, func(t *testing.T) {
// 			// t.Parallel()

// 			var writeSink bytes.Buffer
// 			writeSink.Grow(int(tt.payloadSize))
// 			payload := make([]byte, tt.payloadSize)
// 			writeLimiter := NewWriter(&writeSink, tt.bps)
// 			tt.w = writeLimiter
// 			tt.r = bytes.NewReader(payload)
// 			readWriteTest(t, &tt)
// 		})
// 	}
// }

// func TestReader_Read_InIoCopy(t *testing.T) {

// 	for _, tt := range readerWriterTests {
// 		testName := fmt.Sprintf("payload:%s_limit:%s", tt.payloadSize, tt.bps)
// 		if tt.chain != 0 {
// 			testName += "_chain:" + tt.chain.String()
// 		}
// 		t.Run(testName, func(t *testing.T) {
// 			// t.Parallel()

// 			var writeSink bytes.Buffer
// 			writeSink.Grow(int(tt.payloadSize))
// 			payload := make([]byte, tt.payloadSize)
// 			readSource := bytes.NewReader(payload)

// 			r := NewReader(readSource, tt.bps)
// 			if tt.chain != 0 {
// 				r = NewReader(r, tt.chain)
// 			}
// 			tt.r = r
// 			tt.w = &writeSink
// 			readWriteTest(t, &tt)
// 		})
// 	}
// }

// func readWriteTest(t *testing.T, args *readWriteTestBenchConfig) {
// 	copyStartTime := time.Now()
// 	written, err := io.Copy(args.w, args.r)
// 	copyTime := time.Since(copyStartTime)

// 	if (err != nil) != args.wantErr {
// 		t.Errorf("io.Copy error = %v, wantErr %v", err, args.wantErr)
// 		return
// 	}
// 	if written != int64(args.payloadSize) {
// 		t.Errorf("io.Copy write = %v, want %v", written, args.payloadSize)
// 	}

// 	throughput := float64(written) / copyTime.Seconds()
// 	minBps := float64(args.bps) * 0.95
// 	maxBps := float64(args.bps) * 1.05
// 	if !(minBps <= throughput) || !(throughput <= maxBps) {
// 		t.Errorf("throughput = %f, accepted range [%f, %f]", throughput, minBps, maxBps)
// 	}
// }

// Benchamarks for determining optimal optimal bps and burst ratio
var readWriteBenchmarks = []readWriteTestBenchConfig{
	{bps: 128 * B, payloadSize: 200 * B},
	{bps: 1 * KiB, payloadSize: 4 * KiB},
	{bps: 1 * KiB, chain: 800 * B, payloadSize: 4 * KiB, wantErr: false},
	{bps: 250 * KiB, chain: 200 * KiB, payloadSize: 4 * MiB},
	{bps: 250 * KiB, payloadSize: 4 * MiB},
	{bps: 1 * MiB, payloadSize: 4 * MiB},
	// {bps: 250 * MiB, payloadSize: 1 * GiB},
}

func Benchmark_Reader(b *testing.B) {
	var writeSink bytes.Buffer

	for _, bm := range readWriteBenchmarks {
		chainedLimit := ""
		if bm.chain != 0 {
			chainedLimit = bm.chain.String()
		}
		testName := fmt.Sprintf("payload:%10s_limits:[%10s,%10s]", bm.payloadSize, bm.bps, chainedLimit)

		writeSink.Grow(int(bm.payloadSize))
		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			b.ReportAllocs()

			var avgTroughput float64

			for runN := 0; runN < b.N; runN++ {
				writeSink.Reset()
				readLimiter := NewReader(bytes.NewReader(payload), bm.bps)
				if bm.chain != 0 {
					readLimiter = NewReader(readLimiter, bm.chain)
				}
				avgTroughput += readWriteBenchmark(b, readLimiter, &writeSink)
			}

			avgTroughput /= float64(b.N)
			b.ReportMetric(avgTroughput, "Bps")
		})
	}
}

func Benchmark_Writer(b *testing.B) {
	var writeSink bytes.Buffer

	for _, bm := range readWriteBenchmarks {
		chainedLimit := ""
		if bm.chain != 0 {
			chainedLimit = bm.chain.String()
		}
		testName := fmt.Sprintf("payload:%10s_limits:[%10s,%10s]", bm.payloadSize, bm.bps, chainedLimit)

		writeSink.Grow(int(bm.payloadSize))
		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			b.ReportAllocs()

			var avgTroughput float64

			for runN := 0; runN < b.N; runN++ {
				writeSink.Reset()
				payloadReader := bytes.NewReader(payload)
				writeLimiter := NewWriter(&writeSink, bm.bps)
				if bm.chain != 0 {
					writeLimiter = NewWriter(&writeSink, bm.bps)
					writeLimiter = NewWriter(writeLimiter, bm.chain)
				}
				avgTroughput += readWriteBenchmark(b, payloadReader, writeLimiter)
			}

			avgTroughput /= float64(b.N)
			b.ReportMetric(avgTroughput, "Bps")
		})
	}
}

func readWriteBenchmark(b *testing.B, r io.Reader, w io.Writer) (throughput float64) {
	copyStartTime := time.Now()
	written, _ := io.Copy(w, r)
	copyTime := time.Since(copyStartTime)
	return float64(written) / copyTime.Seconds()
}

func TestListener_Accept(t *testing.T) {
	limiter := NewListener(&MockListener{}).(*limitedListener)
	conn, _ := limiter.Accept()

	_, connStored := limiter.connections.Load(conn)
	if !connStored {
		t.Errorf("ListenerLimiter.Accept() should store accepted connections")
	}
}

func TestListener_ConnClose(t *testing.T) {
	listener := NewListener(&MockListener{}).(*limitedListener)
	conn, _ := listener.Accept()
	conn.Close()
	_, connStored := listener.connections.Load(conn)
	if connStored {
		t.Errorf("connection on close should remove itself from a listener")
	}
}

func TestListener_SetLimits_OnAccept(t *testing.T) {
	bpsPerListenerLimit := 100 * KiB
	bpsPerConnLimit := 2 * KiB

	listener := NewListener(&MockListener{}).(*limitedListener)
	listener.SetBandwithLimits(bpsPerListenerLimit, bpsPerConnLimit)

	gotPerListenerLimit, gotPerConnLimit := listener.BandwithLimits()
	if bpsPerListenerLimit != gotPerListenerLimit ||
		bpsPerConnLimit != gotPerConnLimit {
		t.Errorf("Listener.SetLimits limits sets incorrectly")
	}

	conn, _ := listener.Accept()
	limitedConn := conn.(*limitedConn)
	if bpsPerConnLimit != limitedConn.BandwithLimit() {
		t.Errorf("Listener.SetLimits did not set conn bps limit")
	}
}

func TestListener_SetLimits_PostAccept(t *testing.T) {
	listener := NewListener(&MockListener{})

	conn, _ := listener.Accept()
	limitedConn := conn.(*limitedConn)

	bpsPerListenerLimit := 100 * KiB
	bpsPerConnLimit := 2 * KiB
	listener.SetBandwithLimits(bpsPerListenerLimit, bpsPerConnLimit)

	if bpsPerConnLimit != limitedConn.BandwithLimit() {
		t.Errorf("Listener.SetLimits did not set conn bps limit")
	}
}

func TestConn_Read(t *testing.T) {
	type given struct {
		payload string
		bps     ByteSize
	}
	type expect struct {
		fullRead bool
		err      error
	}
	tests := []struct {
		given  given
		expect expect
	}{
		{
			given{payload: "", bps: 0 * KiB},
			expect{fullRead: true, err: ErrNoBurstAvilable},
		}, {
			given{payload: "", bps: 1 * KiB},
			expect{fullRead: true, err: nil},
		}, {
			given{payload: "01234567890", bps: 0 * KiB},
			expect{fullRead: false, err: ErrNoBurstAvilable},
		}, {
			given{payload: "01234567890", bps: 1 * KiB},
			expect{fullRead: true, err: nil},
		}, {
			given{payload: "01234567890", bps: 1 * B},
			expect{fullRead: false, err: nil},
		},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("payload:<%s>_bps:%v", tt.given.payload, tt.given.bps)
		t.Run(name, func(t *testing.T) {
			sourceConn := MockConn{B: *bytes.NewBuffer([]byte(tt.given.payload))}
			expectN := sourceConn.B.Len()

			conn := NewConn(&sourceConn, tt.given.bps)

			readBuff := make([]byte, expectN)
			n, err := conn.Read(readBuff)
			fullRead := (n == expectN)

			if !errors.Is(err, tt.expect.err) {
				t.Errorf("Conn.Read() error = %v, want err %v", err, tt.expect.err)
			}
			if tt.expect.fullRead && !fullRead {
				t.Errorf("Conn.Read() = %v, want %v", n, expectN)
			}
			if !tt.expect.fullRead && fullRead {
				t.Errorf("Conn.Read() = %v, did not expect full read", n)
			}
		})
	}
}

func TestConn_Read_OnBpsChange(t *testing.T) {
	type given struct {
		payload       string
		bpsChangeFrom ByteSize
		bpsChangeTo   ByteSize
	}
	type expect struct {
		fullRead bool
		err1     error
		err2     error
	}
	tests := []struct {
		given  given
		expect expect
	}{
		{
			given{payload: "", bpsChangeFrom: 0 * KiB, bpsChangeTo: 1 * KiB},
			expect{fullRead: true, err1: ErrNoBurstAvilable, err2: nil},
		}, {
			given{payload: "", bpsChangeFrom: 1 * KiB, bpsChangeTo: 0 * KiB},
			expect{fullRead: true, err1: nil, err2: ErrNoBurstAvilable},
		}, {
			given{payload: "01234567890", bpsChangeFrom: 0 * KiB, bpsChangeTo: 1 * KiB},
			expect{fullRead: true, err1: ErrNoBurstAvilable, err2: nil},
		}, {
			given{payload: "01234567890", bpsChangeFrom: 1 * KiB, bpsChangeTo: 0 * KiB},
			expect{fullRead: true, err1: nil, err2: ErrNoBurstAvilable},
		}, {
			given{payload: "01234567890", bpsChangeFrom: 1 * KiB, bpsChangeTo: 2 * KiB},
			expect{fullRead: true, err1: nil, err2: io.EOF},
		},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("payload:<%s>_bpsFrom:%v_bpsTo:%v",
			tt.given.payload, tt.given.bpsChangeFrom, tt.given.bpsChangeTo)

		t.Run(name, func(t *testing.T) {
			sourceConn := MockConn{B: *bytes.NewBuffer([]byte(tt.given.payload))}
			expectN := sourceConn.B.Len()
			conn := NewConn(&sourceConn, tt.given.bpsChangeFrom)

			readBuff := make([]byte, expectN)
			n, err := conn.Read(readBuff)
			allReadsSize := n

			if !errors.Is(err, tt.expect.err1) {
				t.Errorf("Conn.Read() error = %v, want err %v", err, tt.expect.err1)
			}

			conn.SetBandwithLimit(tt.given.bpsChangeTo)

			n, err = conn.Read(readBuff)
			allReadsSize += n
			fullRead := (allReadsSize == expectN)

			if !errors.Is(err, tt.expect.err2) {
				t.Errorf("Conn.Read() error = %v, want err %v", err, tt.expect.err2)
			}
			if tt.expect.fullRead && !fullRead {
				t.Errorf("Conn.Read() = %v, want %v", n, expectN)
			}
			if !tt.expect.fullRead && fullRead {
				t.Errorf("Conn.Read() = %v, did not expect full read", n)
			}
		})
	}
}

type MockListener struct{}

func (l *MockListener) Accept() (net.Conn, error) { return &MockConn{}, nil }
func (l *MockListener) Close() error              { return nil }
func (l *MockListener) Addr() net.Addr            { return nil }

type MockConn struct {
	B bytes.Buffer
}

func (mc *MockConn) Read(b []byte) (n int, err error) {
	return mc.B.Read(b)
}
func (mc *MockConn) Write(b []byte) (n int, err error)  { return }
func (mc *MockConn) Close() error                       { return nil }
func (mc *MockConn) LocalAddr() net.Addr                { return nil }
func (mc *MockConn) RemoteAddr() net.Addr               { return nil }
func (mc *MockConn) SetDeadline(t time.Time) error      { return nil }
func (mc *MockConn) SetReadDeadline(t time.Time) error  { return nil }
func (ms *MockConn) SetWriteDeadline(t time.Time) error { return nil }

func assertEqual(tb testing.TB, x, y interface{}, fmt string, args ...interface{}) {
	if !reflect.DeepEqual(x, y) {
		tb.Errorf(fmt, args...)
	}
}
