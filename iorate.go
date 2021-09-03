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

type ByteSize uint64

const (
	B ByteSize = 1 << (10 * iota)
	KiB
	MiB
	GiB
	TiB
	PiB

	KB ByteSize = 1000
	MB          = KB * 1000
	GB          = MB * 1000
	TB          = GB * 1000
	PB          = TB * 1000
	EB          = TB * 1000
)

var siUnitsOrdered = []ByteSize{B, KiB, MiB, GiB, TiB, PiB}
var siUnitsNames = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}

func (b ByteSize) String() string {
	var pos int
	for i, unit := range siUnitsOrdered {
		if b/unit == 0 {
			break
		}
		pos = i
	}
	val := float64(b) / float64(siUnitsOrdered[pos])
	return fmt.Sprintf("%0.2f%s", val, siUnitsNames[pos])
}

// Reader controlls how often reads are allowed to happen.
type Reader struct {
	*BandwidthLimiter
	src io.Reader
}

// NewReadLimiter creates new ReadLimiter.
func NewReader(r io.Reader, bps ByteSize) *Reader {
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
func NewWriter(w io.Writer, bps ByteSize) *Writer {
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
	SetBandwithLimits(bpsPerListener, bpsPerConn ByteSize)

	// BandwithLimits return the per listener and per connection
	// bandwith limits.
	BandwithLimits() (listenerLimit, connLimit ByteSize)
}

// Listener controlls how often read and writes bandwitch for
// connections spawned by a listener.
type limitedListener struct {
	net.Listener
	connections     sync.Map
	limitsMu        sync.RWMutex
	readLimiter     BandwidthLimiter
	writeLimiter    BandwidthLimiter
	bpsPerConnLimit ByteSize
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
func (l *limitedListener) SetBandwithLimits(bpsPerListener, bpsPerConn ByteSize) {
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
func (l *limitedListener) BandwithLimits() (listenerLimit, connLimit ByteSize) {
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
	SetBandwithLimit(bps ByteSize)

	// BandwithLimit returns the current bandwith limit.
	BandwithLimit() ByteSize
}

type limitedConn struct {
	net.Conn
	r        *Reader
	w        *Writer
	mu       sync.RWMutex
	listener *limitedListener
}

// NewConn creates new rate  limiting connection.
func NewConn(nc net.Conn, bps ByteSize) Conn {
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
func (conn *limitedConn) SetBandwithLimit(bps ByteSize) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.r.SetBandwithLimit(bps)
	conn.w.SetBandwithLimit(bps)
}

// BandwithLimit returns the bandwith limit.
func (conn *limitedConn) BandwithLimit() ByteSize {
	return conn.r.BandwithLimit()
}

type BandwidthLimiter struct {
	limiter  rate.Limiter
	mu       sync.RWMutex
	bpsLimit ByteSize
}

func NewBandwidthLimiter(bps ByteSize) *BandwidthLimiter {
	bl := new(BandwidthLimiter)
	bl.limiter = *rate.NewLimiter(rate.Limit(bps), int(burstSizePolicy(bps)))
	return bl
}

func (bl *BandwidthLimiter) SetBandwithLimit(bps ByteSize) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.bpsLimit = bps
	bl.limiter.SetLimit(rate.Limit(bps))
	bl.limiter.SetBurst(int(burstSizePolicy(bps)))
}

func (bl *BandwidthLimiter) BandwithLimit() ByteSize {
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
	BurstRateRecomended = 32 * KiB
	BurstRateMax        = 5 * MiB
)

func burstSizePolicy(bps ByteSize) ByteSize {
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
