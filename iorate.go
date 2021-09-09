package iorate

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

var ErrNoBurstAvilable = fmt.Errorf("burst value too low for i/o operation")

type Bandwidth float64

const (
	Bps Bandwidth = 1 << (10 * iota)
	KBps
	MBps
	GBps
	TBps
	PBps
)

var bpsUnits = []Bandwidth{Bps, KBps, MBps, GBps, TBps, PBps}
var bpsUnitNames = []string{"Bps", "KBps", "MBps", "GBps", "TBps", "PBps"}

// String formats bandwidth value in human readable format.
func (b Bandwidth) String() string {
	var pos int
	for i, unit := range bpsUnits {
		if int(b/unit) == 0 {
			break
		}
		pos = i
	}
	return fmt.Sprintf("%0.2f%s", b/bpsUnits[pos], bpsUnitNames[pos])
}

// LimitedReader controlls how often reads are allowed to happen.
type LimitedReader interface {
	io.Reader

	// SetReadLimit sets the bandwidth limtis for future read.
	SetReadLimit(bps Bandwidth)

	// ReadLimit returns the read i/o bandwidth limit.
	ReadLimit() Bandwidth
}

// LimitedWriter controlls how often writes are allowed to happen.
type LimitedWriter interface {
	io.Writer

	// SetWriteLimit sets the bandwidth limit for future write i/o ops.
	SetWriteLimit(bps Bandwidth)

	// WriteLimit returns the write i/o bandwidth limit.
	WriteLimit() Bandwidth
}

type limitedReader struct {
	limiter *limiter
	src     io.Reader
}

// NewLimitedReader creates new LimitedReader.
func NewLimitedReader(r io.Reader, bps Bandwidth) LimitedReader {
	return &limitedReader{src: r, limiter: newLimiter(bps)}
}

// Read calls wrapped io.Reader, but limits the throughput.
func (r *limitedReader) Read(b []byte) (n int, err error) {
	// Chained limited readerrs should not wait needlesly
	allowed := len(b)
	limitedSrc, limitedSrcOk := r.src.(*limitedReader)
	if limitedSrcOk && limitedSrc.limiter.Burst() < allowed {
		allowed = limitedSrc.limiter.Burst()
	}
	allowed, err = r.limiter.waitBandwidth(allowed)
	if err != nil {
		return 0, err
	}
	return r.src.Read(b[:allowed])
}

func (r *limitedReader) SetReadLimit(bps Bandwidth) { r.limiter.SetLimit(bps) }
func (r *limitedReader) ReadLimit() Bandwidth       { return r.limiter.Limit() }

// Writer controlls how often writes are allowed to happen.
type limitedWriter struct {
	limiter *limiter
	dst     io.Writer
}

// NewLimitedWriter creates new Writer and sets
func NewLimitedWriter(w io.Writer, bps Bandwidth) LimitedWriter {
	return &limitedWriter{dst: w, limiter: newLimiter(bps)}

}

// Write calls wrapped io.Writer, but limits the throughput.
func (w *limitedWriter) Write(b []byte) (n int, err error) {
	reader := limitedReader{
		src:     bytes.NewReader(b),
		limiter: w.limiter,
	}
	written, err := io.Copy(w.dst, &reader)
	return int(written), err
}

// ReadFrom writes directly from a source reader into wrapped  io.Writer,
// but limits the throughput.
//
// Enables the io.Copy to skip the temporary buffer.
func (w *limitedWriter) ReadFrom(r io.Reader) (n int64, err error) {
	reader := limitedReader{src: r, limiter: w.limiter}
	return io.Copy(w.dst, &reader)
}

func (w *limitedWriter) SetWriteLimit(bps Bandwidth) { w.limiter.SetLimit(bps) }
func (w *limitedWriter) WriteLimit() Bandwidth       { return w.limiter.Limit() }

// LimitedListener controlls how often read and writes bandwitch for
// connections spawned by a listener.
type LimitedListener interface {
	// Listener implements net.Listener interface.
	net.Listener

	// SetLimits sets the per listener and per connection bandwith limits
	SetLimits(listenerLimit, connLimit Bandwidth)

	// Limits return the per listener and per connection  bandwith limits.
	Limits() (listenerLimit, connLimit Bandwidth)
}

// limitedListener controlls how often read and writes bandwitch for
// connections spawned by a listener.
type limitedListener struct {
	net.Listener
	connections  sync.Map
	limitsMu     sync.RWMutex
	readLimiter  limiter
	writeLimiter limiter
	connLimit    Bandwidth
}

func NewLimitedListener(l net.Listener) LimitedListener {
	return &limitedListener{Listener: l}
}

// Accept waits for and returns the next connection to the listener.
func (l *limitedListener) Accept() (net.Conn, error) {
	var limitedConn *limitedConn
	nc, err := l.Listener.Accept()
	if err == nil {
		l.limitsMu.RLock()
		limitedConn = newLimitedListenerConn(nc, l)
		l.connections.Store(limitedConn, 1)
		l.limitsMu.RUnlock()
	}
	return limitedConn, err
}

func (l *limitedListener) onConnClose(conn Conn) {
	l.connections.Delete(conn)
}

// SetLimits sets the per listener and per connection bandwith limits.
func (l *limitedListener) SetLimits(listenerLimit, connLimit Bandwidth) {
	l.limitsMu.Lock()
	defer l.limitsMu.Unlock()
	l.readLimiter.SetLimit(listenerLimit)
	l.writeLimiter.SetLimit(listenerLimit)
	l.connLimit = connLimit
	l.connections.Range(func(key, value interface{}) bool {
		conn := key.(Conn)
		conn.SetLimit(connLimit)
		return true
	})
}

// Limits return the per listener and per connection bandwith limits.
func (l *limitedListener) Limits() (listenerLimit, connLimit Bandwidth) {
	l.limitsMu.RLock()
	defer l.limitsMu.RUnlock()
	return l.readLimiter.Limit(), l.connLimit
}

// Conn is a generic stream-oriented network connection
// with Bandwith  limiting  capability.
type Conn interface {
	// Conn implements the net.Conn interface.
	net.Conn

	// SetLimit sets the bandwith limit for future i/o ops.
	SetLimit(bps Bandwidth)

	// Limit returns the current bandwith limit.
	Limit() Bandwidth
}

type limitedConn struct {
	net.Conn
	r        LimitedReader
	w        LimitedWriter
	mu       sync.RWMutex
	listener *limitedListener
}

// NewLimitedConn creates new rate  limiting connection.
func NewLimitedConn(nc net.Conn, bps Bandwidth) Conn {
	return &limitedConn{
		Conn: nc,
		r:    NewLimitedReader(nc, bps),
		w:    NewLimitedWriter(nc, bps),
	}
}

func newLimitedListenerConn(nc net.Conn, l *limitedListener) *limitedConn {
	listenerReadLimiter := &limitedReader{src: nc, limiter: &l.readLimiter}
	listenerWriteLimiter := &limitedWriter{dst: nc, limiter: &l.writeLimiter}
	return &limitedConn{
		Conn:     nc,
		listener: l,
		r:        NewLimitedReader(listenerReadLimiter, l.connLimit),
		w:        NewLimitedWriter(listenerWriteLimiter, l.connLimit),
	}
}

// Read implements the net.Conn Read method.
func (conn *limitedConn) Read(b []byte) (n int, err error) {
	return conn.r.Read(b)
}

// Write implements the net.Conn Write method.
func (conn *limitedConn) Write(b []byte) (n int, err error) {
	return conn.w.Write(b)
}

// Close closes the connection.
func (conn *limitedConn) Close() error {
	if conn.listener != nil {
		conn.listener.onConnClose(conn)
	}
	return conn.Conn.Close()
}

// SetLimit sets the bandwith limit.
func (conn *limitedConn) SetLimit(bps Bandwidth) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.r.SetReadLimit(bps)
	conn.w.SetWriteLimit(bps)
}

// Limit returns the bandwith limit.
func (conn *limitedConn) Limit() Bandwidth { return conn.r.ReadLimit() }

type limiter struct {
	impl     rate.Limiter
	mu       sync.RWMutex
	bpsLimit Bandwidth
}

func newLimiter(bps Bandwidth) *limiter {
	return &limiter{
		impl:     *rate.NewLimiter(rate.Limit(bps), int(burstSizePolicy(bps))),
		bpsLimit: bps,
	}
}

func (l *limiter) SetLimit(bps Bandwidth) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bpsLimit = bps
	l.impl.SetLimit(rate.Limit(bps))
	l.impl.SetBurst(int(burstSizePolicy(bps)))
}

func (l *limiter) SetBurst(burst int) {
	l.impl.SetBurst(burst)
}

func (l *limiter) Burst() int {
	return l.impl.Burst()
}

func (bl *limiter) Limit() Bandwidth {
	bl.mu.RLock()
	defer bl.mu.RUnlock()
	return bl.bpsLimit
}

func (bl *limiter) waitBandwidth(want int) (int, error) {
	bandwidth := bl.impl.Burst()
	if bandwidth == 0 {
		return 0, ErrNoBurstAvilable
	}
	if want < bandwidth {
		bandwidth = want
	}
	if rsv := bl.impl.ReserveN(time.Now(), bandwidth); rsv.OK() {
		time.Sleep(rsv.Delay())
		return bandwidth, nil
	}
	return 0, ErrNoBurstAvilable
}

const (
	burstRateMTU = 512 * Bps
)

func burstSizePolicy(bps Bandwidth) Bandwidth {
	burst := bps
	if bps > burstRateMTU {
		// decent size of io
		burst = bps * 0.00025
		if burst < burstRateMTU {
			burst = burstRateMTU
			// burst = 32 * KBps
		}
	} else {
		// small io
		for i := 1; i < 4; i++ {
			if x := burst / 2; x >= 16 {
				burst = x
			}
		}
	}
	// fmt.Printf("burstSizePolicy %s\n", burst)
	return burst
}
