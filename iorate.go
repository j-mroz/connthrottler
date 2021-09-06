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

// Reader controlls how often reads are allowed to happen.
type Reader struct {
	*BandwidthLimiter
	src io.Reader
}

// NewReadLimiter creates new ReadLimiter.
func NewReader(r io.Reader, bps Bandwidth) *Reader {
	return &Reader{src: r, BandwidthLimiter: NewBandwidthLimiter(bps)}
}

// NewReadLimiter creates new ReadLimiter.
func NewReaderWithSharedLimit(r io.Reader, limiter *BandwidthLimiter) *Reader {
	return &Reader{src: r, BandwidthLimiter: limiter}
}

// Read calls wrapped io.Reader, but limits the throughput.
func (r *Reader) Read(b []byte) (n int, err error) {
	allowed, err := r.waitBandwidth(len(b))
	if err != nil {
		return 0, err
	}
	return r.src.Read(b[:allowed])
}

// Writer controlls how often writes are allowed to happen.
type Writer struct {
	*BandwidthLimiter
	writer io.Writer
}

// NewWriter creates new Writer and sets
func NewWriter(w io.Writer, bps Bandwidth) *Writer {
	return &Writer{writer: w, BandwidthLimiter: NewBandwidthLimiter(bps)}
}

// NewWriter creates new Writer and sets
func NewWriterWithSharedLimit(w io.Writer, limiter *BandwidthLimiter) *Writer {
	return &Writer{writer: w, BandwidthLimiter: limiter}
}

// Write calls wrapped io.Writer, but limits the throughput.
func (wl *Writer) Write(b []byte) (n int, err error) {
	reader := Reader{
		src:              bytes.NewReader(b),
		BandwidthLimiter: wl.BandwidthLimiter,
	}
	written, err := io.Copy(wl.writer, &reader)
	return int(written), err
}

func (wl *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	reader := Reader{
		src:              r,
		BandwidthLimiter: wl.BandwidthLimiter,
	}
	return io.Copy(wl.writer, &reader)
}

type Listener interface {
	// Listener implements net.Listener interface.
	net.Listener

	// SetBandwithLimits sets the per listener and per connection
	// bandwith limits
	SetBandwithLimits(bpsPerListener, bpsPerConn Bandwidth)

	// BandwithLimits return the per listener and per connection
	// bandwith limits.
	BandwithLimits() (listenerLimit, connLimit Bandwidth)
}

// Listener controlls how often read and writes bandwitch for
// connections spawned by a listener.
type limitedListener struct {
	net.Listener
	connections     sync.Map
	limitsMu        sync.RWMutex
	readLimiter     BandwidthLimiter
	writeLimiter    BandwidthLimiter
	bpsPerConnLimit Bandwidth
}

func NewListener(l net.Listener) Listener {
	return &limitedListener{Listener: l}
}

// Accept waits for and returns the next connection to the listener.
func (l *limitedListener) Accept() (net.Conn, error) {
	var limitedConn *limitedConn
	nc, err := l.Listener.Accept()
	if err == nil {
		l.limitsMu.RLock()
		limitedConn = newConnWithListener(nc, l)
		limitedConn.SetBandwithLimit(l.bpsPerConnLimit)
		l.connections.Store(limitedConn, 1)
		l.limitsMu.RUnlock()
	}
	return limitedConn, err
}

func (l *limitedListener) onConnClose(conn Conn) {
	l.connections.Delete(conn)
}

// SetBandwithLimits sets the per listener and per connection bandwith limits.
func (l *limitedListener) SetBandwithLimits(bpsPerListener, bpsPerConn Bandwidth) {
	l.limitsMu.Lock()
	defer l.limitsMu.Unlock()
	l.readLimiter.SetBandwithLimit(bpsPerListener)
	l.writeLimiter.SetBandwithLimit(bpsPerListener)
	l.bpsPerConnLimit = bpsPerConn
	l.connections.Range(func(key, value interface{}) bool {
		conn := key.(Conn)
		conn.SetBandwithLimit(bpsPerConn)
		return false
	})
}

// BandwithLimits return the per listener and per connection bandwith limits.
func (l *limitedListener) BandwithLimits() (listenerLimit, connLimit Bandwidth) {
	l.limitsMu.Lock()
	defer l.limitsMu.Unlock()
	return l.readLimiter.BandwithLimit(), l.bpsPerConnLimit
}

// Conn is a generic stream-oriented network connection
// with Bandwith  limiting  capability.
type Conn interface {
	// Conn implements the net.Conn interface.
	net.Conn

	// SetBandwithLimit sets the bandwith limit for future i/o ops.
	SetBandwithLimit(bps Bandwidth)

	// BandwithLimit returns the current bandwith limit.
	BandwithLimit() Bandwidth
}

type limitedConn struct {
	net.Conn
	r        *Reader
	w        *Writer
	mu       sync.RWMutex
	listener *limitedListener
}

// NewConn creates new rate  limiting connection.
func NewConn(nc net.Conn, bps Bandwidth) Conn {
	return &limitedConn{
		Conn: nc,
		r:    NewReader(nc, bps),
		w:    NewWriter(nc, bps),
	}
}

func newConnWithListener(nc net.Conn, l *limitedListener) *limitedConn {
	listenerReadLimiter := NewReaderWithSharedLimit(nc, &l.readLimiter)
	listenerWriteLimiter := NewWriterWithSharedLimit(nc, &l.writeLimiter)
	return &limitedConn{
		Conn:     nc,
		listener: l,
		r:        NewReader(listenerReadLimiter, l.bpsPerConnLimit),
		w:        NewWriter(listenerWriteLimiter, l.bpsPerConnLimit),
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

// SetBandwithLimit sets the bandwith limit.
func (conn *limitedConn) SetBandwithLimit(bps Bandwidth) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.r.SetBandwithLimit(bps)
	conn.w.SetBandwithLimit(bps)
}

// BandwithLimit returns the bandwith limit.
func (conn *limitedConn) BandwithLimit() Bandwidth {
	return conn.r.BandwithLimit()
}

type BandwidthLimiter struct {
	limiter  rate.Limiter
	mu       sync.RWMutex
	bpsLimit Bandwidth
}

func NewBandwidthLimiter(bps Bandwidth) *BandwidthLimiter {
	bl := new(BandwidthLimiter)
	bl.limiter = *rate.NewLimiter(rate.Limit(bps), int(burstSizePolicy(bps)))
	return bl
}

func (bl *BandwidthLimiter) SetBandwithLimit(bps Bandwidth) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.bpsLimit = bps
	bl.limiter.SetLimit(rate.Limit(bps))
	bl.limiter.SetBurst(int(burstSizePolicy(bps)))
}

func (bl *BandwidthLimiter) BandwithLimit() Bandwidth {
	bl.mu.RLock()
	defer bl.mu.RUnlock()
	return bl.bpsLimit
}

func (bl *BandwidthLimiter) waitBandwidth(want int) (int, error) {
	// bl.mu.RLock()
	// defer bl.mu.RUnlock()
	bandwidth := bl.limiter.Burst()
	if bandwidth == 0 {
		return 0, ErrNoBurstAvilable
	}
	if want < bandwidth {
		bandwidth = want
	}
	if rsv := bl.limiter.ReserveN(time.Now(), bandwidth); rsv.OK() {
		time.Sleep(rsv.Delay())
		return bandwidth, nil
	}
	return 0, ErrNoBurstAvilable
}

// Default behavior for burst
const (
	BurstRateRecomended = 32 * KBps
	BurstRateMax        = 5 * MBps
)

func burstSizePolicy(bps Bandwidth) Bandwidth {
	burst := BurstRateRecomended

	if burst > bps {
		burst = bps
		// For better smoothing of traffic aim for at least 8 burst in second
		for i := 0; i < 4; i++ {
			if x := burst / 2; x >= 16 {
				burst = x
			}
		}
	}
	for burst*100 < bps {
		burst *= 10
	}
	if BurstRateMax < burst {
		burst = BurstRateMax
	}
	return burst
}
