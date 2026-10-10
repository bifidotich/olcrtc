package vp8channel

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// Diagnostics for measuring what the SFU actually carries: our send rate and,
// per remote track, what reaches us. They only count and log, and only with
// OLCRTC_DIAG=1.

const diagPeriod = 2 * time.Second

var diagEnabled = os.Getenv("OLCRTC_DIAG") == "1" //nolint:gochecknoglobals // read once at startup

var (
	diagTxSamples atomic.Uint64
	diagTxBytes   atomic.Uint64
	diagTxOnce    sync.Once
)

// diagCountTx records one sample handed to the VP8 track.
func diagCountTx(n int) {
	if !diagEnabled {
		return
	}
	diagTxSamples.Add(1)
	diagTxBytes.Add(uint64(n)) //nolint:gosec // n is a slice length
	diagTxOnce.Do(func() { go diagTxLoop() })
}

func diagTxLoop() {
	ticker := time.NewTicker(diagPeriod)
	defer ticker.Stop()
	var lastSamples, lastBytes uint64
	for range ticker.C {
		samples, bytes := diagTxSamples.Load(), diagTxBytes.Load()
		ds, db := samples-lastSamples, bytes-lastBytes
		lastSamples, lastBytes = samples, bytes
		if ds == 0 {
			continue
		}
		logger.Infof("vp8diag tx: %.0f samples/s %.1f KB/s",
			float64(ds)/diagPeriod.Seconds(), float64(db)/1024/diagPeriod.Seconds())
	}
}

// diagRx counts what one remote track delivers. Owned by readVP8Track.
type diagRx struct {
	rtp, rtpBytes, frames, frameBytes int
	since                             time.Time
}

func (d *diagRx) maybeLog(trackID string) {
	if !diagEnabled {
		return
	}
	now := time.Now()
	if d.since.IsZero() {
		d.since = now
		return
	}
	elapsed := now.Sub(d.since)
	if elapsed < diagPeriod {
		return
	}
	if d.rtp > 0 {
		secs := elapsed.Seconds()
		logger.Infof("vp8diag rx track=%.8s: %.0f rtp/s %.1f KB/s -> %.0f frames/s %.1f KB/s",
			trackID, float64(d.rtp)/secs, float64(d.rtpBytes)/1024/secs,
			float64(d.frames)/secs, float64(d.frameBytes)/1024/secs)
	}
	*d = diagRx{since: now}
}
