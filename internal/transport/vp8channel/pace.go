package vp8channel

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// Pacing of the bulk data plane. Since ~2026-10-01 Telemost's SFU forwards an
// overloaded stream at a fraction of what we send and drops the rest, which
// turns KCP's retransmissions into a collapse: unpaced, a 5 MB download never
// completed. Holding the send rate near what the SFU forwards keeps loss low
// and every transfer completes. The defaults are what measured best on the
// live SFU; the environment overrides them for experiments without a rebuild:
//
//	OLCRTC_VP8_PACE_KBPS  data-plane send rate in KB/s (0: unpaced)
//	OLCRTC_KCP_WND        KCP send/receive window in segments
//	OLCRTC_VP8_INTERFRAME 0: send data samples as VP8 keyframes again
const (
	paceEnvRate      = "OLCRTC_VP8_PACE_KBPS"
	kcpEnvWnd        = "OLCRTC_KCP_WND"
	interframeEnvVar = "OLCRTC_VP8_INTERFRAME"

	defaultPaceKBps = 192
	// pacedKCPWindow replaces kcpSndWnd/kcpRcvWnd while pacing. KCP runs
	// without congestion control and keeps its whole window queued, so the
	// window is also the queue: 4096 segments at 192 KB/s would wait ~27 s,
	// far past any RTO. 256 segments is ~1.7 s and still covers the RTT.
	pacedKCPWindow = 256
)

// envInt returns the non-negative integer in name, and whether it was set.
func envInt(name string) (int, bool) {
	s, set := os.LookupEnv(name)
	if !set {
		return 0, false
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

func paceKBps() int {
	if v, set := envInt(paceEnvRate); set {
		return v
	}
	return defaultPaceKBps
}

// interframeFromEnv reports whether data samples are tagged as VP8
// interframes. Every sample opens with the keepalive keyframe header, so
// untagged the SFU sees hundreds of keyframes a second where a real camera
// sends one every few seconds, and Telemost forwards that at ~33 KB/s at best.
// Bare keepalives (the periodic and PLI-requested ones) stay keyframes. The
// receiver parses the header by offset, so the tag is wire-compatible with an
// untagged peer.
func interframeFromEnv() bool {
	v, set := envInt(interframeEnvVar)
	return !set || v != 0
}

// kcpWindow returns the KCP window: the OLCRTC_KCP_WND override, else the
// paced window while pacing, else the unpaced default.
func kcpWindow() (int, int) {
	if w, _ := envInt(kcpEnvWnd); w > 0 {
		return w, w
	}
	if paceKBps() > 0 {
		return pacedKCPWindow, pacedKCPWindow
	}
	return kcpSndWnd, kcpRcvWnd
}

// pacer is a token bucket over data-plane bytes. All data writers of one
// transport share it, since they share one video track. A nil pacer allows
// everything. The bucket may go into debt by one sample, so a sample is never
// split or held back once it has started.
type pacer struct {
	mu     sync.Mutex
	rate   float64 // bytes per second
	burst  float64 // bytes
	tokens float64
	last   time.Time
}

func newPacerFromEnv() *pacer {
	kbps := paceKBps()
	if kbps == 0 {
		return nil
	}
	rate := float64(kbps) * 1024
	logger.Infof("vp8channel: pacing data plane at %d KB/s", kbps)
	// 100 ms worth of bytes, but at least two full samples.
	burst := max(rate/10, 2*singleSampleLimit)
	return &pacer{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

// ready reports whether the bucket has credit for another sample.
func (b *pacer) ready() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens = min(b.tokens+now.Sub(b.last).Seconds()*b.rate, b.burst)
	b.last = now
	return b.tokens > 0
}

// spend charges n bytes against the bucket.
func (b *pacer) spend(n int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens -= float64(n)
	b.mu.Unlock()
}
