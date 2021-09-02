package connthrottler

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"golang.org/x/time/rate"
)

var ErrNoBurstAvilable = fmt.Errorf("Burst value too low to proceed")

type BytesUnit uint64

const (
	B   BytesUnit = 1
	KiB BytesUnit = 1 << 10
	MiB BytesUnit = 1 << 20
	GiB BytesUnit = 1 << 30
	TiB BytesUnit = 1 << 40
	KB  BytesUnit = 10 << 3
	MB  BytesUnit = 10 << 4
	GB  BytesUnit = 10 << 5
	TB  BytesUnit = 10 << 6
)

var siUnitsOrdered = []BytesUnit{B, KiB, MiB, GiB, TiB}
var siUnitsNames = []string{"B", "KiB", "MiB", "GiB", "TiB"}

func (b BytesUnit) String() string {
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
// Implements io.Writer interface
type WriteLimiter struct {
	writier io.Writer
	// limiter *rate.Limiter
	limit BytesUnit
	burst BytesUnit
}

// NewWriteLimiter creates new WriteLimiter and sets
// bytes per second and burst values in the limiter.
func NewWriteLimiter(w io.Writer, bps, burst BytesUnit) *WriteLimiter {
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
	// TODO: creating a new reader each time is not really best performance choice.
	rl := NewReadLimiter(bytes.NewReader(b), wl.limit, wl.burst)
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
func NewReadLimiter(r io.Reader, bps, burst BytesUnit) *ReadLimiter {
	l := rate.NewLimiter(rate.Limit(bps), int(burst))
	return &ReadLimiter{reader: r, limiter: l}
}

// Write method limits the bytes written to at most configured amount
// burst bytes and at the same time tries to maitian the configured bps.
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
