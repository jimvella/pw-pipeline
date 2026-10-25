package middleware

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"plane.watch/lib/tracker"
	"plane.watch/lib/tracker/beast"
	"plane.watch/lib/tracker/sbs1"
)

var (
	// DF17 from 7C49F8 and from 48FC83, as the Beast producer emits them.
	tapBeast7C49F8 = []byte{0x1a, 0x33, 0x22, 0x1b, 0x54, 0xac, 0xc2, 0xe9, 0x28, 0x8d, 0x7c, 0x49, 0xf8, 0x58, 0x41, 0xd2, 0x6c, 0xca, 0x39, 0x33, 0xe4, 0x1e, 0xcf}
	tapBeast48FC83 = []byte{0x1A, 0x33, 0x14, 0x93, 0xFF, 0x2D, 0xD3, 0xC7, 0x62, 0x8D, 0x48, 0xFC, 0x83, 0x1C, 0x4D, 0x04, 0xCD, 0x14, 0x48, 0x20, 0x72, 0x37, 0xC3}
)

type (
	published struct {
		subject, typ, tag string
		data              []byte
	}
	recordingPublisher struct {
		mu   sync.Mutex
		msgs []published
	}
)

func (p *recordingPublisher) PublishMsg(m *nats.Msg) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, published{
		subject: m.Subject,
		typ:     m.Header.Get("type"),
		tag:     m.Header.Get("tag"),
		data:    bytes.Clone(m.Data),
	})
	return nil
}

func (p *recordingPublisher) all() []published {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]published(nil), p.msgs...)
}

func beastEvent(t *testing.T, raw []byte, tag string) (*beast.Frame, tracker.FrameEvent) {
	t.Helper()
	f, err := beast.NewFrame(bytes.Clone(raw), false)
	if err != nil {
		t.Fatalf("NewFrame: %v", err)
	}
	if err = f.Decode(); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return f, tracker.NewFrameEvent(f, &tracker.FrameSource{Tag: tag})
}

// drain starts one worker on a tap built with none, and waits for it to empty the queue.
func drain(tap *IngestTap) {
	tap.queueWg.Add(1)
	go tap.processQueue()
	tap.Stop()
}

func conditionCount(tap *IngestTap) int {
	set := tap.conds.Load()
	n := len(set.anyTag)
	for _, cs := range set.byTag {
		n += len(cs)
	}
	return n
}

func TestTapIdleSendsNothing(t *testing.T) {
	pub := &recordingPublisher{}
	tap := newIngestTap(pub, 0)
	_, fe := beastEvent(t, tapBeast7C49F8, "feeder-a")
	if got := tap.Handle(&fe); got != fe.Frame() {
		t.Errorf("Handle must return the frame unchanged")
	}
	if len(tap.queue) != 0 {
		t.Errorf("idle tap queued %d frames", len(tap.queue))
	}
	drain(tap)
	if n := len(pub.all()); n != 0 {
		t.Errorf("idle tap published %d frames", n)
	}
}

func TestTapRepeatedAddIsIdempotent(t *testing.T) {
	pub := &recordingPublisher{}
	tap := newIngestTap(pub, 0)
	c := condition{subject: "out"}
	if !tap.add(c) {
		t.Errorf("first add should report a new condition")
	}
	for i := 0; i < 59; i++ {
		if tap.add(c) {
			t.Fatalf("repeat add %d reported a new condition", i)
		}
	}
	if n := conditionCount(tap); n != 1 {
		t.Fatalf("conditions = %d, want 1", n)
	}
	_, fe := beastEvent(t, tapBeast7C49F8, "feeder-a")
	tap.Handle(&fe)
	drain(tap)
	if n := len(pub.all()); n != 1 {
		t.Errorf("published %d copies, want 1", n)
	}
}

func TestTapRemove(t *testing.T) {
	tap := newIngestTap(&recordingPublisher{}, 0)
	defer tap.Stop()
	a := condition{subject: "out", apiKey: "a"}
	b := condition{subject: "out", apiKey: "b"}
	c := condition{subject: "out"}
	tap.add(a)
	tap.add(b)
	tap.add(c)
	if !tap.remove(b) {
		t.Errorf("remove of a present condition should report true")
	}
	if tap.remove(b) {
		t.Errorf("second remove should report false")
	}
	if n := conditionCount(tap); n != 2 {
		t.Fatalf("conditions = %d, want 2", n)
	}
	set := tap.conds.Load()
	if len(set.byTag["b"]) != 0 || len(set.byTag["a"]) != 1 || len(set.anyTag) != 1 {
		t.Errorf("wrong conditions left after remove: %+v", set)
	}
	tap.remove(a)
	tap.remove(c)
	if !tap.conds.Load().empty() {
		t.Errorf("tap should be idle after removing every condition")
	}
}

func TestTapFiltersBeforeQueuing(t *testing.T) {
	tap := newIngestTap(&recordingPublisher{}, 0)
	tap.add(condition{subject: "out", apiKey: "wanted"})

	// Far more unwanted frames than the queue holds: none may be queued.
	for i := 0; i < 5*tapQueueLen; i++ {
		_, fe := beastEvent(t, tapBeast7C49F8, "other")
		tap.Handle(&fe)
	}
	if len(tap.queue) != 0 {
		t.Fatalf("queued %d frames that match no condition", len(tap.queue))
	}
	_, fe := beastEvent(t, tapBeast7C49F8, "wanted")
	tap.Handle(&fe)
	if len(tap.queue) != 1 {
		t.Errorf("matching frame not queued")
	}
	tap.Stop()
}

func TestTapIcaoFilter(t *testing.T) {
	pub := &recordingPublisher{}
	tap := newIngestTap(pub, 0)
	tap.add(condition{subject: "out", icao: 0x48FC83})
	_, fe1 := beastEvent(t, tapBeast7C49F8, "a")
	_, fe2 := beastEvent(t, tapBeast48FC83, "a")
	tap.Handle(&fe1)
	tap.Handle(&fe2)
	drain(tap)
	msgs := pub.all()
	if len(msgs) != 1 || !bytes.Equal(msgs[0].data, tapBeast48FC83) {
		t.Errorf("icao filter sent %d frames, want only 48FC83's", len(msgs))
	}
}

func TestTapFullQueueDoesNotBlock(t *testing.T) {
	tap := newIngestTap(&recordingPublisher{}, 0) // no workers, so the queue fills
	tap.add(condition{subject: "out"})
	for i := 0; i < tapQueueLen+7; i++ {
		_, fe := beastEvent(t, tapBeast7C49F8, "a")
		tap.Handle(&fe) // would hang here if a full queue blocked the pipeline
	}
	if len(tap.queue) != tapQueueLen {
		t.Errorf("queue holds %d frames, want %d", len(tap.queue), tapQueueLen)
	}
	tap.Stop()
}

func TestTapCopiesPooledBeastFrame(t *testing.T) {
	beast.UsePoolAllocator = true
	defer func() { beast.UsePoolAllocator = false }()

	pub := &recordingPublisher{}
	tap := newIngestTap(pub, 0)
	tap.add(condition{subject: "out"})

	f, fe := beastEvent(t, tapBeast7C49F8, "feeder-a")
	tap.Handle(&fe)
	// What the tracker does after the middleware: release the frame, which zeroes its
	// bytes, and reuse it for the next message.
	beast.Release(f)
	for i := 0; i < 10; i++ {
		_, _ = beastEvent(t, tapBeast48FC83, "feeder-b")
	}
	drain(tap)

	msgs := pub.all()
	if len(msgs) != 1 {
		t.Fatalf("published %d frames, want 1", len(msgs))
	}
	m := msgs[0]
	if !bytes.Equal(m.data, tapBeast7C49F8) {
		t.Errorf("published % X, want the frame as it was when handled % X", m.data, tapBeast7C49F8)
	}
	if m.subject != "out" || m.typ != "beast" || m.tag != "feeder-a" {
		t.Errorf("got subject %q type %q tag %q", m.subject, m.typ, m.tag)
	}
}

func TestTapSbs1Frame(t *testing.T) {
	pub := &recordingPublisher{}
	tap := newIngestTap(pub, 0)
	tap.add(condition{subject: "out", apiKey: "mlat-mux"})
	line := "MSG,3,1,1,7C6DB8,1,2026/10/25,01:02:03.456,2026/10/25,01:02:04.000,,3400,,,-31.950000,115.860000,,,,,,"
	f := sbs1.NewFrame(line)
	if err := f.Decode(); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	fe := tracker.NewFrameEvent(f, &tracker.FrameSource{Tag: "mlat-mux"})
	tap.Handle(&fe)
	drain(tap)
	msgs := pub.all()
	if len(msgs) != 1 || string(msgs[0].data) != line || msgs[0].typ != "sbs1" {
		t.Errorf("got %+v", msgs)
	}
}

func TestTapExpiry(t *testing.T) {
	tap := newIngestTap(&recordingPublisher{}, 0)
	defer tap.Stop()
	t0 := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	now := t0
	tap.now = func() time.Time { return now }

	stale := condition{subject: "stale"}
	kept := condition{subject: "kept"}
	tap.add(stale)
	tap.add(kept)

	now = t0.Add(3 * time.Minute)
	tap.add(kept) // refresh
	now = t0.Add(TapConditionExpiry - time.Second)
	tap.expire()
	if n := conditionCount(tap); n != 2 {
		t.Fatalf("conditions = %d before expiry, want 2", n)
	}

	now = t0.Add(TapConditionExpiry)
	tap.expire()
	set := tap.conds.Load()
	if len(set.anyTag) != 1 || set.anyTag[0] != kept {
		t.Fatalf("after expiry got %+v, want only the refreshed condition", set.anyTag)
	}

	now = t0.Add(3*time.Minute + TapConditionExpiry)
	tap.expire()
	if !tap.conds.Load().empty() {
		t.Errorf("refreshed condition should expire 5 minutes after its last add")
	}
}

func tapRequest(action, subject, apiKey, icao string) *nats.Msg {
	m := nats.NewMsg(NatsAPIv1PwIngestTap)
	m.Header.Set("action", action)
	m.Header.Set("subject", subject)
	m.Header.Set("api-key", apiKey)
	m.Header.Set("icao", icao)
	return m
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	old := log.Logger
	log.Logger = zerolog.New(zerolog.SyncWriter(buf)).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = old })
	return buf
}

func logLevels(t *testing.T, buf *bytes.Buffer) []string {
	t.Helper()
	var levels []string
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry struct{ Level string }
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		levels = append(levels, entry.Level)
	}
	buf.Reset()
	return levels
}

func TestTapRequestLogging(t *testing.T) {
	buf := captureLog(t)
	tap := newIngestTap(&recordingPublisher{}, 0)
	defer tap.Stop()

	tap.requestHandler(tapRequest("add", "out", "", ""))
	if got := logLevels(t, buf); len(got) != 1 || got[0] != "info" {
		t.Errorf("new condition logged %v, want one info line", got)
	}
	for i := 0; i < 3; i++ {
		tap.requestHandler(tapRequest("add", "out", "", ""))
	}
	if got := logLevels(t, buf); len(got) != 3 || got[0] != "debug" || got[2] != "debug" {
		t.Errorf("refreshes logged %v, want debug lines only", got)
	}
	tap.requestHandler(tapRequest("remove", "out", "", ""))
	if got := logLevels(t, buf); len(got) != 1 || got[0] != "info" {
		t.Errorf("remove logged %v, want one info line", got)
	}
}

func TestTapRejectsBadRequests(t *testing.T) {
	captureLog(t)
	tap := newIngestTap(&recordingPublisher{}, 0)
	defer tap.Stop()
	tap.requestHandler(tapRequest("add", "out", "", "not-hex"))
	tap.requestHandler(tapRequest("add", "", "", ""))
	if !tap.conds.Load().empty() {
		t.Errorf("a bad icao or a missing subject must not add a condition (it would match every frame)")
	}
	tap.requestHandler(tapRequest("add", "out", "", "7c49f8"))
	set := tap.conds.Load()
	if len(set.anyTag) != 1 || set.anyTag[0].icao != 0x7C49F8 || !set.withIcao {
		t.Errorf("icao condition not parsed: %+v", set)
	}
}

// Run with -race: requests change the conditions while frames flow through Handle and the workers.
func TestTapConcurrentRequestsAndFrames(t *testing.T) {
	captureLog(t)
	pub := &recordingPublisher{}
	tap := newIngestTap(pub, 4)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(tag string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, fe := beastEvent(t, tapBeast7C49F8, tag)
				tap.Handle(&fe)
			}
		}([]string{"a", "b", "c", "d"}[i])
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				tap.requestHandler(tapRequest("add", "out", "a", ""))
				tap.requestHandler(tapRequest("add", "out", "", ""))
				tap.expire()
				tap.requestHandler(tapRequest("remove", "out", "", ""))
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	tap.Stop()
	if n := conditionCount(tap); n != 1 {
		t.Errorf("conditions = %d, want the api-key one", n)
	}
}
