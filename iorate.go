package iorate

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

// WriteLimiter controlls how often writes are allowed to happen.
// Do not use same writer from multiple goroutines.
type WriteLimiter struct {
	writier     io.Writer
	readLimiter ReadLimiter
}

// NewWriteLimiter creates new WriteLimiter and sets
// bytes per second and burst values in the limiter.
func NewWriteLimiter(w io.Writer, bps, burst ByteSize) *WriteLimiter {
	return &WriteLimiter{writier: w, readLimiter: *NewReadLimiter(nil, bps, burst)}
}

// SetBandwithLimits sets bytes per second and burst limits
func (wl *WriteLimiter) SetBandwithLimits(bps ByteSize, burst ByteSize) {
	wl.readLimiter.SetBandwithLimits(bps, burst)
}

// Write calls wrapped io.Writer, but limits the throughput.
func (wl *WriteLimiter) Write(b []byte) (n int, err error) {
	wl.readLimiter.reader = bytes.NewReader(b)
	n2, err := io.Copy(wl.writier, &wl.readLimiter)
	return int(n2), err
}

// ReadLimiter controlls how often reads are allowed to happen.
type ReadLimiter struct {
	reader  io.Reader
	limiter *rate.Limiter
}

// NewReadLimiter creates new ReadLimiter and sets
// bytes per second and burst values in the limiter.
func NewReadLimiter(r io.Reader, bps, burst ByteSize) *ReadLimiter {
	l := rate.NewLimiter(rate.Limit(bps), int(burst))
	return &ReadLimiter{reader: r, limiter: l}
}

func (rl *ReadLimiter) SetBandwithLimits(bps ByteSize, burst ByteSize) {
	rl.limiter.SetLimit(rate.Limit(bps))
	rl.limiter.SetBurst(int(burst))
}

func (rl *ReadLimiter) readSize(proposal int) int {
	if rl.limiter.Burst() < proposal {
		return rl.limiter.Burst()
	}
	return proposal
}

func (rl *ReadLimiter) waitBandwidth(size int) bool {
	r := rl.limiter.ReserveN(time.Now(), size)
	if r.OK() {
		time.Sleep(r.Delay())
	}
	return r.OK()
}

// Read calls wrapped io.Reader, but limits the throughput.
func (rl *ReadLimiter) Read(b []byte) (n int, err error) {
	readSize := rl.readSize(len(b))
	if rl.waitBandwidth(readSize) {
		return rl.reader.Read(b[:readSize])
	}
	return 0, ErrNoBurstAvilable
}
