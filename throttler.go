package connthrottler

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"golang.org/x/time/rate"
)

var ErrNoBurstAvilable = fmt.Errorf("burst value too low to proceed")

type ByteSize uint64

const (
	_            = iota
	B   ByteSize = 1
	KiB ByteSize = 1 << (10 * iota)
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

// WriteLimiter decorates io.Writer with a limitter
// with provided bytes per second value and max burst.
// Implements io.Writer interface.
// Please note that resulting throughput bps depend on the value of
// burst size and bps limit. Too small or too big burst compared to bps limit
// causes the throughput to drop.
type WriteLimiter struct {
	writier io.Writer
	// limiter *rate.Limiter
	limit ByteSize
	burst ByteSize
}

// NewWriteLimiter creates new WriteLimiter and sets
// bytes per second and burst values in the limiter.
func NewWriteLimiter(w io.Writer, bps, burst ByteSize) *WriteLimiter {
	// l := rate.NewLimiter(rate.Limit(bps), int(burst))
	// return &WriteLimiter{writier: w, limiter: l, limit: bps, burst: burst}
	return &WriteLimiter{writier: w, limit: bps, burst: burst}

}

// Write method limits the bytes written to at most configured amount
// burst bytes and at the same time tries to maintain the configured bps.
func (wl *WriteLimiter) Write(b []byte) (n int, err error) {

	// Instead of throttling the writes, reuse the read limitter
	// and throttle the reads from the source.
	// It's convenient because throttling single writes
	// cases the io.Copy to return Err.ShortRead, while throttling writes
	// does not have that problem.
	// It seems io.Copy assumes the io.Writer.Write will consume every
	// single byte provided.
	bytesReader := bytes.NewReader(b)
	rl := NewReadLimiter(bytesReader, wl.limit, wl.burst)
	n2, err := io.Copy(wl.writier, rl)
	return int(n2), err
}

// ReadLimiter decorates io.Reader with a limitter
// with provided bytes per second value and max burst.
// Implements io.Writer interface
type ReadLimiter struct {
	reader  io.Reader
	limiter *rate.Limiter
}

// NewWriteLimiter creates new WriteLimiter and sets
// bytes per second and burst values in the limiter.
func NewReadLimiter(r io.Reader, bps, burst ByteSize) *ReadLimiter {
	l := rate.NewLimiter(rate.Limit(bps), int(burst))
	return &ReadLimiter{reader: r, limiter: l}
}

// Write method limits the bytes written to at most configured amount
// burst bytes and at the same time tries to maintain the configured bps.
func (rl *ReadLimiter) Read(b []byte) (n int, err error) {
	maxReadSize := rl.limiter.Burst()
	readSize := len(b)
	if maxReadSize < readSize {
		readSize = maxReadSize
	}

	// Assume one token is one byte
	r := rl.limiter.ReserveN(time.Now(), readSize)
	if r.OK() {
		time.Sleep(r.Delay())
		return rl.reader.Read(b[:readSize])
	}

	return 0, ErrNoBurstAvilable
}
