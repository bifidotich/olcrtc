package goolom

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// rtcpDiagPeriod is how often the RTCP feedback summary is logged.
const rtcpDiagPeriod = 2 * time.Second

// rtcpDiag aggregates the RTCP feedback the SFU sends about our published
// track, to measure the bandwidth it estimates for us and the loss it sees.
// Diagnostic only: it does not influence sending.
type rtcpDiag struct {
	mu sync.Mutex

	types map[string]int

	remb  float64 // last REMB estimate, bit/s
	rembN int

	rrN         int
	rrLossMax   uint8 // worst fraction lost in the period, /256
	rrTotalLost uint32
	rrJitter    uint32

	nackLost int

	twccFb    int
	twccTotal int
	twccRecv  int
}

// newRTCPDiag returns nil unless OLCRTC_DIAG=1; a nil rtcpDiag does nothing.
func newRTCPDiag() *rtcpDiag {
	if os.Getenv("OLCRTC_DIAG") != "1" {
		return nil
	}
	return &rtcpDiag{types: make(map[string]int)}
}

func (d *rtcpDiag) observe(pkt rtcp.Packet) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.types[strings.TrimPrefix(fmt.Sprintf("%T", pkt), "*rtcp.")]++
	switch p := pkt.(type) {
	case *rtcp.ReceiverEstimatedMaximumBitrate:
		d.remb = float64(p.Bitrate)
		d.rembN++
	case *rtcp.ReceiverReport:
		for _, rep := range p.Reports {
			d.rrN++
			d.rrLossMax = max(d.rrLossMax, rep.FractionLost)
			d.rrTotalLost = rep.TotalLost
			d.rrJitter = rep.Jitter
		}
	case *rtcp.TransportLayerNack:
		for _, pair := range p.Nacks {
			d.nackLost += len(pair.PacketList())
		}
	case *rtcp.TransportLayerCC:
		d.twccFb++
		d.twccTotal += int(p.PacketStatusCount)
		d.twccRecv += len(p.RecvDeltas)
	}
}

func (d *rtcpDiag) flush(label string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.types) == 0 {
		return
	}
	names := make([]string, 0, len(d.types))
	for name, n := range d.types {
		names = append(names, fmt.Sprintf("%s=%d", name, n))
	}
	sort.Strings(names)
	twcc := "-"
	if d.twccTotal > 0 {
		twcc = fmt.Sprintf("%d/%d (%.0f%% recv)", d.twccRecv, d.twccTotal,
			100*float64(d.twccRecv)/float64(d.twccTotal))
	}
	remb := "-"
	if d.rembN > 0 {
		remb = fmt.Sprintf("%.0f kbit/s (n=%d)", d.remb/1000, d.rembN)
	}
	logger.Infof("rtcpdiag %s: remb=%s rr_loss_max=%d%% rr_total_lost=%d rr_jitter=%d nack_lost=%d twcc=%s types[%s]",
		label, remb, int(d.rrLossMax)*100/256, d.rrTotalLost, d.rrJitter, d.nackLost, twcc,
		strings.Join(names, " "))
	d.types = make(map[string]int)
	d.rembN, d.rrN, d.rrLossMax, d.nackLost = 0, 0, 0, 0
	d.twccFb, d.twccTotal, d.twccRecv = 0, 0, 0
}

// run logs a summary every rtcpDiagPeriod until done is closed.
func (d *rtcpDiag) run(label string, done <-chan struct{}) {
	if d == nil {
		return
	}
	ticker := time.NewTicker(rtcpDiagPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			d.flush(label)
			return
		case <-ticker.C:
			d.flush(label)
		}
	}
}
