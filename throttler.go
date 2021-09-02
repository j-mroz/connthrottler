package connthrottler

import (
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

type WriteLimiter struct {
	writier io.Writer
	limiter *rate.Limiter
}

// NewWriteLimiter decorates io.Writter with a limitter
// with provided bytes per second value and max burst.
func NewWriteLimiter(w io.Writer, bps, burst BytesUnit) *WriteLimiter {
	l := rate.NewLimiter(rate.Limit(bps), int(burst))
	return &WriteLimiter{writier: w, limiter: l}
}

// Write method limits the bytes written to at most configured amount
// burst bytes and at the same time tries to maitian the configured bps.
func (wl *WriteLimiter) Write(b []byte) (n int, err error) {
	maxWriteSize := wl.limiter.Burst()
	writeSize := len(b)
	if maxWriteSize < writeSize {
		writeSize = maxWriteSize
	}

	r := wl.limiter.ReserveN(time.Now(), writeSize)
	if r.OK() {
		time.Sleep(r.Delay())
		return wl.writier.Write(b[:writeSize])
	}
	return 0, ErrNoBurstAvilable
}
