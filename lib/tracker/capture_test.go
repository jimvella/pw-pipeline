package tracker

import (
	"bufio"
	"io"
	"os"
	"testing"
	"time"

	"plane.watch/lib/tracker/mode_s"
)

// TestNavFromBeastCapture replays a raw Beast capture through the tracker and
// reports how many aircraft yield navigation and enhanced surveillance data.
// It runs only when PW_BEAST_CAPTURE names a capture file; PW_BEAST_CAPTURE_START
// (RFC 3339) gives the time of its first frame, else the current time is
// used. The Perth capture in the Read5 repository
// (collector/testdata/beast/beast-30005-20261022T004216Z.bin, start
// 2026-10-22T00:42:16Z) carries 50,339 frames from about 32 aircraft.
func TestNavFromBeastCapture(t *testing.T) {
	path := os.Getenv("PW_BEAST_CAPTURE")
	if path == "" {
		t.Skip("set PW_BEAST_CAPTURE to a Beast capture file")
	}
	start := time.Now()
	if s := os.Getenv("PW_BEAST_CAPTURE_START"); s != "" {
		var err error
		if start, err = time.Parse(time.RFC3339, s); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	trk := NewTracker()
	var frames, decoded int
	navSeen := map[uint32]bool{}
	ehsSeen := map[uint32]bool{}
	modesSeen := map[uint32]bool{}
	var firstTicks uint64
	err = readBeast(bufio.NewReader(f), func(ticks uint64, msg []byte) {
		frames++
		if firstTicks == 0 {
			firstTicks = ticks
		}
		ts := start.Add(time.Duration(ticks-firstTicks) * time.Second / 12_000_000)
		frame := mode_s.NewFrameFromBytes(ticks, msg, ts)
		if err := frame.Decode(); err != nil {
			return
		}
		decoded++
		if frame.Icao() == 0 {
			return
		}
		p := trk.GetPlane(frame.Icao())
		p.HandleModeSFrame(&frame, nil)
		if nav, ok := p.Nav(); ok {
			if nav.SelectedAltitudeMCP != nil || nav.SelectedAltitudeFMS != nil {
				navSeen[frame.Icao()] = true
			}
			if nav.Modes != nil {
				modesSeen[frame.Icao()] = true
			}
		}
		if _, ok := p.EHS(); ok {
			ehsSeen[frame.Icao()] = true
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d frames, %d decoded, %d aircraft; selected altitude from %d, nav modes from %d, EHS from %d",
		frames, decoded, trk.numPlanes(), len(navSeen), len(modesSeen), len(ehsSeen))
	if len(navSeen) == 0 || len(ehsSeen) == 0 {
		t.Errorf("no aircraft yielded nav (%d) or EHS (%d) data", len(navSeen), len(ehsSeen))
	}
}

// readBeast walks a Beast binary stream and calls fn for every Mode S frame
// (types 0x32 and 0x33) with its 12 MHz timestamp and message bytes.
func readBeast(r *bufio.Reader, fn func(ticks uint64, msg []byte)) error {
	readByte := func() (byte, error) {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == 0x1a {
			// an escaped 0x1a inside a frame
			if next, err := r.Peek(1); err == nil && next[0] == 0x1a {
				_, _ = r.ReadByte()
			}
		}
		return b, nil
	}
	for {
		// find a frame start
		b, err := r.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if b != 0x1a {
			continue
		}
		typ, err := r.ReadByte()
		if err != nil {
			return nil
		}
		var msgLen int
		switch typ {
		case 0x31, 0x34:
			msgLen = 2
		case 0x32:
			msgLen = 7
		case 0x33:
			msgLen = 14
		default:
			continue
		}
		var ticks uint64
		for i := 0; i < 6; i++ {
			b, err := readByte()
			if err != nil {
				return nil
			}
			ticks = ticks<<8 | uint64(b)
		}
		if _, err := readByte(); err != nil { // signal level
			return nil
		}
		msg := make([]byte, msgLen)
		for i := range msg {
			if msg[i], err = readByte(); err != nil {
				return nil
			}
		}
		if typ == 0x32 || typ == 0x33 {
			fn(ticks, msg)
		}
	}
}
