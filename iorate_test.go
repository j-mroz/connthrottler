package iorate

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type ByteSize uint

const (
	B  ByteSize = 1
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
)

var byteUnits = []ByteSize{B, KB, MB, GB, TB, PB}
var byteNames = []string{"B", "K", "MB", "GB", "TB", "PB"}

func (b ByteSize) String() string {
	var pos int
	for i, unit := range byteUnits {
		if b/unit == 0 {
			break
		}
		pos = i
	}
	return fmt.Sprintf("%0.2f%s", float64(b)/float64(byteUnits[pos]), byteNames[pos])
}

func TestBytesUnit_String(t *testing.T) {
	tests := []struct {
		name string
		b    Bandwidth
		want string
	}{
		{"1Bps", 1 * Bps, "1.00Bps"},
		{"1KBps", 1 * KBps, "1.00KBps"},
		{"1GBps", 1 * MBps, "1.00MBps"},
		{"1GBps", 1 * GBps, "1.00GBps"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.b.String(),
				"BytesUnit.String() returned wrong value")
		})
	}
}

func TestReader_Read_WrappedReaders(t *testing.T) {
	payload := []byte("0123456789")
	expectRead := 3

	reader1 := NewLimitedReader(bytes.NewReader(payload), 5*Bps)
	reader2 := NewLimitedReader(reader1, 3*Bps)
	reader3 := NewLimitedReader(reader2, 10*Bps)

	buff := make([]byte, 32*1024)
	read, err := reader3.Read(buff)

	assert.Equalf(t, expectRead, read, "Reader.Read() wrong read size")
	assert.NoError(t, err)
	assert.Equal(t, payload[:read], buff[:read])
}

func TestWriter_Write_WrappedWriters(t *testing.T) {
	var writeSink bytes.Buffer

	payload := []byte("0123456789")

	//  Writter is expected to write all
	expectWritten := 10

	writer1 := NewLimitedWriter(&writeSink, 15*Bps)
	writer2 := NewLimitedWriter(writer1, 5*Bps)
	writer3 := NewLimitedWriter(writer2, 10*Bps)

	written, err := writer3.Write(payload)

	assert.Equalf(t, expectWritten, written, "Write.Write() wrong read size")
	assert.Equal(t, payload, writeSink.Bytes())

	assert.NoError(t, err)
}

func TestWriter_Write(t *testing.T) {
	type given struct {
		payload string
		bps     Bandwidth
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
			given{payload: "", bps: 0 * KBps},
			expect{fullWrite: false, err: ErrNoBurstAvilable},
		}, {
			given{payload: "", bps: 1 * KBps},
			expect{fullWrite: true, err: nil},
		}, {
			given{payload: "01234567890", bps: 5 * Bps},
			expect{fullWrite: false, err: nil},
		}, {
			given{payload: "01234567890", bps: 1 * KBps},
			expect{fullWrite: true, err: nil},
		},
	}

	for _, tt := range tests {
		testName := fmt.Sprintf("payload:%s_limit:%s",
			tt.given.payload, tt.given.bps)

		t.Run(testName, func(t *testing.T) {
			var writeSink bytes.Buffer
			limitedWriter := NewLimitedWriter(&writeSink, tt.given.bps)

			payload := []byte(tt.given.payload)
			n, err := limitedWriter.Write([]byte(payload))
			expectN := len(payload)

			assert.Equalf(t, expectN, n, "Writer.Writer() wrong write size")
			assert.ErrorIs(t, err, tt.expect.err)
		})
	}
}

type readWriteBenchConfig struct {
	bps         Bandwidth
	chain       Bandwidth // enabled if not 0
	payloadSize ByteSize
	wantErr     bool
	r           io.Reader
	w           io.Writer
}

// Benchamarks for determining optimal optimal bps and burst ratio
var readWriteBenchmarks = []readWriteBenchConfig{
	{bps: 128 * Bps, payloadSize: 200 * B},
	{bps: 1 * KBps, payloadSize: 4 * KB},
	{bps: 1 * KBps, chain: 800 * Bps, payloadSize: 4 * KB, wantErr: false},
	{bps: 250 * KBps, chain: 200 * KBps, payloadSize: 4 * MB},
	{bps: 250 * KBps, payloadSize: 4 * MB},
	{bps: 1 * MBps, payloadSize: 4 * MB},
	// {bps: 250 * MBps, payloadSize: 1 * GBps},
}

func Benchmark_Reader(b *testing.B) {
	var writeSink bytes.Buffer

	for _, bm := range readWriteBenchmarks {
		chainedLimit := ""
		if bm.chain != 0 {
			chainedLimit = bm.chain.String()
		}
		testName := fmt.Sprintf("payload:%s_limits:%s,%s",
			bm.payloadSize, bm.bps, chainedLimit)

		writeSink.Grow(int(bm.payloadSize))
		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			b.ReportAllocs()

			var avgTroughput float64

			for runN := 0; runN < b.N; runN++ {
				writeSink.Reset()
				limitedReader := NewLimitedReader(bytes.NewReader(payload), bm.bps)
				if bm.chain != 0 {
					limitedReader = NewLimitedReader(limitedReader, bm.chain)
				}
				avgTroughput += readWriteBenchmark(b, limitedReader, &writeSink)
			}

			avgTroughput /= float64(b.N)
			b.ReportMetric(avgTroughput, "Bps")
		})
	}
}

func Benchmark_Writer(b *testing.B) {

	for _, bm := range readWriteBenchmarks {
		chainedLimit := ""
		if bm.chain != 0 {
			chainedLimit = bm.chain.String()
		}
		testName := fmt.Sprintf("payload:%s_limits:%s,%s",
			bm.payloadSize, bm.bps, chainedLimit)

		payload := make([]byte, bm.payloadSize)

		b.Run(testName, func(b *testing.B) {
			var writeSink bytes.Buffer
			writeSink.Grow(int(bm.payloadSize))

			var avgTroughput float64

			b.ReportAllocs()
			for runN := 0; runN < b.N; runN++ {
				writeSink.Reset()
				payloadReader := bytes.NewReader(payload)
				limitedWriter := NewLimitedWriter(&writeSink, bm.bps)
				if bm.chain != 0 {
					limitedWriter = NewLimitedWriter(&writeSink, bm.bps)
					limitedWriter = NewLimitedWriter(limitedWriter, bm.chain)
				}
				avgTroughput += readWriteBenchmark(b, payloadReader, limitedWriter)
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
	limiter := NewLimitedListener(&MockListener{}).(*limitedListener)
	conn, _ := limiter.Accept()

	_, connStored := limiter.connections.Load(conn)
	assert.Truef(t, connStored, "ListenerLimiter.Accept() should store accepted connections")
}

func TestListener_ConnClose(t *testing.T) {
	listener := NewLimitedListener(&MockListener{}).(*limitedListener)
	conn, _ := listener.Accept()
	conn.Close()
	_, connStored := listener.connections.Load(conn)
	assert.Falsef(t, connStored, "connection on close should remove itself from a listener")
}

func TestListener_SetLimits_BeforeAccept(t *testing.T) {
	bpsPerListenerLimit := 100 * KBps
	bpsPerConnLimit := 2 * KBps

	listener := NewLimitedListener(&MockListener{}).(*limitedListener)
	listener.SetLimits(bpsPerListenerLimit, bpsPerConnLimit)

	gotPerListenerLimit, gotPerConnLimit := listener.Limits()

	assert.Equalf(t, bpsPerListenerLimit, gotPerListenerLimit, "bpsPerListener")
	assert.Equalf(t, bpsPerConnLimit, gotPerConnLimit, "bpsPerConn")

	conn, _ := listener.Accept()
	limitedConn := conn.(*limitedConn)
	assert.Equalf(t, bpsPerConnLimit, limitedConn.Limit(),
		"Listener.SetLimits did not set conn bps limit")
}

func TestListener_SetLimits_PostAccept(t *testing.T) {
	listener := NewLimitedListener(&MockListener{})

	conn, _ := listener.Accept()
	limitedConn := conn.(*limitedConn)

	bpsPerListenerLimit := 100 * KBps
	bpsPerConnLimit := 2 * KBps
	listener.SetLimits(bpsPerListenerLimit, bpsPerConnLimit)

	assert.Equalf(t, bpsPerConnLimit, limitedConn.Limit(),
		"Listener.SetLimits did not set conn bps limit")

}

func TestConn_Read(t *testing.T) {
	type given struct {
		payload string
		bps     Bandwidth
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
			given{payload: "", bps: 0 * KBps},
			expect{fullRead: true, err: ErrNoBurstAvilable},
		}, {
			given{payload: "", bps: 1 * KBps},
			expect{fullRead: true, err: nil},
		}, {
			given{payload: "01234567890", bps: 0 * KBps},
			expect{fullRead: false, err: ErrNoBurstAvilable},
		}, {
			given{payload: "01234567890", bps: 1 * KBps},
			expect{fullRead: true, err: nil},
		}, {
			given{payload: "01234567890", bps: 1 * Bps},
			expect{fullRead: false, err: nil},
		},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("payload:<%s>_bps:%v", tt.given.payload, tt.given.bps)
		t.Run(name, func(t *testing.T) {
			sourceConn := MockConn{B: *bytes.NewBuffer([]byte(tt.given.payload))}
			expectN := sourceConn.B.Len()

			conn := NewLimitedConn(&sourceConn, tt.given.bps)

			readBuff := make([]byte, expectN)
			n, err := conn.Read(readBuff)
			fullRead := (n == expectN)

			assert.ErrorIs(t, err, tt.expect.err, "Conn.Read() error")
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
		bpsChangeFrom Bandwidth
		bpsChangeTo   Bandwidth
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
			given{payload: "", bpsChangeFrom: 0 * KBps, bpsChangeTo: 1 * KBps},
			expect{fullRead: true, err1: ErrNoBurstAvilable, err2: io.EOF},
		}, {
			given{payload: "", bpsChangeFrom: 1 * KBps, bpsChangeTo: 0 * KBps},
			expect{fullRead: true, err1: io.EOF, err2: ErrNoBurstAvilable},
		}, {
			given{payload: "01234567890", bpsChangeFrom: 0 * KBps, bpsChangeTo: 1 * KBps},
			expect{fullRead: true, err1: ErrNoBurstAvilable, err2: nil},
		}, {
			given{payload: "01234567890", bpsChangeFrom: 1 * KBps, bpsChangeTo: 0 * KBps},
			expect{fullRead: true, err1: nil, err2: ErrNoBurstAvilable},
		}, {
			given{payload: "01234567890", bpsChangeFrom: 1 * KBps, bpsChangeTo: 2 * KBps},
			expect{fullRead: true, err1: nil, err2: io.EOF},
		},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("payload:<%s>_bpsFrom:%v_bpsTo:%v",
			tt.given.payload, tt.given.bpsChangeFrom, tt.given.bpsChangeTo)

		t.Run(name, func(t *testing.T) {
			payloadBuff := []byte(tt.given.payload)
			mockConn := MockConn{B: *bytes.NewBuffer(payloadBuff)}
			expectReadSize := mockConn.B.Len()

			conn := NewLimitedConn(&mockConn, tt.given.bpsChangeFrom)

			readBuff := make([]byte, expectReadSize+10)
			n, err := conn.Read(readBuff)
			allReadsSize := n

			assert.ErrorIs(t, err, tt.expect.err1, "Conn.Read() error")

			conn.SetLimit(tt.given.bpsChangeTo)

			n, err = conn.Read(readBuff[n:])
			allReadsSize += n
			fullRead := (allReadsSize == expectReadSize)

			assert.ErrorIs(t, err, tt.expect.err2, "Conn.Read() error")
			if tt.expect.fullRead && !fullRead {
				t.Errorf("Conn.Read() = %v, want %v", n, expectReadSize)
			}
			if !tt.expect.fullRead && fullRead {
				t.Errorf("Conn.Read() = %v, did not expect full read", n)
			}

			assert.Equal(t, payloadBuff[:allReadsSize], readBuff[:allReadsSize])
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
	tb.Helper()
	if !reflect.DeepEqual(x, y) {
		tb.Errorf(fmt, args...)
	}
}
