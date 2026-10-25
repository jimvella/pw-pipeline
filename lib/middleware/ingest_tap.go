package middleware

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"plane.watch/lib/nats_io"
	"plane.watch/lib/randstr"
	"plane.watch/lib/tracker"
	"plane.watch/lib/tracker/beast"
	"plane.watch/lib/tracker/mode_s"
	"plane.watch/lib/tracker/sbs1"
)

const NatsAPIv1PwIngestTap = "v1.pw-ingest.tap" //nolint:gosec

const (
	// TapConditionExpiry is how long a condition lives without another "add" for it.
	// A client that keeps a tap open repeats its "add" well within this (cmd/ingest_tap: every minute).
	TapConditionExpiry = 5 * time.Minute

	tapQueueLen      = 100
	tapWorkers       = 8
	tapExpiryCheck   = 30 * time.Second
	tapErrorLogEvery = 10 * time.Second

	// beastMaxFrameLen is the longest Beast frame the producer emits, escapes removed:
	// esc, type, 6 byte timestamp, signal, 14 byte Mode S message.
	beastMaxFrameLen = 23
)

const (
	tapTypeBeast uint8 = iota + 1
	tapTypeAvr
	tapTypeSbs1
)

var tapTypeNames = [...]string{tapTypeBeast: "beast", tapTypeAvr: "avr", tapTypeSbs1: "sbs1"}

type (
	publisher interface {
		PublishMsg(m *nats.Msg) error
	}

	// IngestTap sends a copy of the frames passing through it to NATS subjects that
	// clients ask for on NatsAPIv1PwIngestTap.
	//
	// The frame path never takes a lock: Handle and the workers only load conds, an
	// immutable snapshot that requests replace whole (copy on write).
	IngestTap struct {
		conds atomic.Pointer[conditionSet]

		mu        sync.Mutex // guards lastAdded and serialises snapshot changes
		lastAdded map[condition]time.Time
		expiry    time.Duration
		now       func() time.Time

		queue   chan tapItem
		queueWg sync.WaitGroup
		stop    chan struct{}
		stopWg  sync.WaitGroup

		lastErrorLog atomic.Int64 // unix nanos

		pub       publisher
		natsQueue string
		sub       *nats.Subscription
	}

	condition struct {
		subject string // where matching frames are sent
		apiKey  string // matched against the frame source's Tag; "" matches every source
		icao    uint32 // 0 matches every aircraft
	}

	// conditionSet is never modified once stored in IngestTap.conds.
	conditionSet struct {
		anyTag   []condition            // conditions without an api-key filter
		byTag    map[string][]condition // conditions with one, by api-key
		withIcao bool                   // some condition filters on icao, so Handle needs the frame's
	}

	// tapItem is a frame copied out of the decode path, so the tracker can release
	// pooled Beast frames as soon as Handle returns.
	tapItem struct {
		raw  [beastMaxFrameLen]byte // beast frames
		n    uint8
		typ  uint8
		icao uint32
		tag  string
		body []byte // avr and sbs1 frames, which are not pooled
	}
)

// NewIngestTap returns the tap middleware, listening for add and remove requests on
// NatsAPIv1PwIngestTap and publishing matching frames through natsServer.
// It returns nil if it cannot subscribe for requests.
func NewIngestTap(natsServer *nats_io.Server) tracker.Middleware {
	tap := newIngestTap(natsServer, tapWorkers)
	var err error
	tap.natsQueue = "pw-ingest-tap-" + randstr.RandString(20)
	tap.sub, err = natsServer.SubscribeReply(NatsAPIv1PwIngestTap, tap.natsQueue, tap.requestHandler)
	if err != nil {
		log.Error().Err(err).Msg("Ingest Tap could not subscribe for requests")
		tap.Stop()
		return nil
	}
	log.Info().Msg("Including Ingest Tap")
	return tap
}

// newIngestTap starts the workers and the expiry check, but does not listen for requests.
func newIngestTap(pub publisher, workers int) *IngestTap {
	tap := &IngestTap{
		lastAdded: make(map[condition]time.Time),
		expiry:    TapConditionExpiry,
		now:       time.Now,
		queue:     make(chan tapItem, tapQueueLen),
		stop:      make(chan struct{}),
		pub:       pub,
	}
	tap.conds.Store(&conditionSet{})
	for i := 0; i < workers; i++ {
		tap.queueWg.Add(1)
		go tap.processQueue()
	}
	tap.stopWg.Add(1)
	go tap.expireLoop()
	return tap
}

// Stop stops taking requests, then publishes what is already queued and returns.
// The tracker calls it once decoding has finished, so Handle is no longer called.
func (tap *IngestTap) Stop() {
	if nil != tap.sub {
		if err := tap.sub.Unsubscribe(); err != nil {
			log.Error().Err(err).Msg("Failed to unsubscribe from nats")
		}
	}
	close(tap.stop)
	tap.stopWg.Wait()
	close(tap.queue)
	tap.queueWg.Wait()
}

// String names the middleware.
func (tap *IngestTap) String() string {
	return "Ingest Tap"
}

// HealthCheckName names the tap in health checks.
func (tap *IngestTap) HealthCheckName() string {
	return "Ingest Tap"
}

// HealthCheck always reports healthy: a tap that cannot keep up drops frames, it never stalls.
func (tap *IngestTap) HealthCheck() bool {
	return true
}

// Handle queues a copy of the frame if any condition wants it. An idle tap costs one
// atomic load and a length check per frame.
func (tap *IngestTap) Handle(fe *tracker.FrameEvent) tracker.Frame {
	frame := fe.Frame()
	set := tap.conds.Load()
	if set.empty() {
		return frame
	}
	var tag string
	if source := fe.Source(); nil != source {
		tag = source.Tag
	}
	var icao uint32
	if set.withIcao && nil != frame {
		icao = frame.Icao()
	}
	if !set.matches(tag, icao) {
		return frame
	}
	item, ok := newTapItem(frame, tag, icao)
	if !ok {
		return frame
	}
	select {
	case tap.queue <- item:
	default:
		// Drop tap data rather than block pipeline
	}
	return frame
}

func newTapItem(frame tracker.Frame, tag string, icao uint32) (tapItem, bool) {
	item := tapItem{tag: tag, icao: icao}
	switch f := frame.(type) {
	case *beast.Frame:
		raw := f.Raw()
		if len(raw) > len(item.raw) {
			log.Debug().Int("len", len(raw)).Msg("Ingest Tap: Beast frame too long, not sent")
			return item, false
		}
		item.n = uint8(copy(item.raw[:], raw))
		item.typ = tapTypeBeast
	case *mode_s.Frame:
		item.body = f.Raw()
		item.typ = tapTypeAvr
	case *sbs1.Frame:
		item.body = f.Raw()
		item.typ = tapTypeSbs1
	default:
		log.Error().Msg("Unknown frame type, cannot send")
		return item, false
	}
	return item, true
}

func (item *tapItem) data() []byte {
	if item.typ == tapTypeBeast {
		return item.raw[:item.n]
	}
	return item.body
}

func (tap *IngestTap) processQueue() {
	defer tap.queueWg.Done()
	// One message per worker, reused for every frame: PublishMsg encodes it before returning.
	typeHeader := []string{""}
	tagHeader := []string{""}
	msg := nats.NewMsg("")
	msg.Header["type"] = typeHeader
	msg.Header["tag"] = tagHeader

	for item := range tap.queue {
		set := tap.conds.Load()
		typeHeader[0] = tapTypeNames[item.typ]
		tagHeader[0] = item.tag
		msg.Data = item.data()
		for _, c := range set.anyTag {
			tap.sendTo(msg, c, item.icao)
		}
		for _, c := range set.byTag[item.tag] {
			tap.sendTo(msg, c, item.icao)
		}
	}
}

func (tap *IngestTap) sendTo(msg *nats.Msg, c condition, icao uint32) {
	if !c.matchIcao(icao) {
		return
	}
	msg.Subject = c.subject
	if err := tap.pub.PublishMsg(msg); nil != err {
		// At network frame rates a NATS outage would otherwise log every frame.
		now := time.Now().UnixNano()
		last := tap.lastErrorLog.Load()
		if now-last >= int64(tapErrorLogEvery) && tap.lastErrorLog.CompareAndSwap(last, now) {
			log.Error().Err(err).Msg("Failed to send tap data")
		}
	}
}

func (tap *IngestTap) requestHandler(msg *nats.Msg) {
	if nil == msg {
		return
	}
	action := msg.Header.Get("action")
	apiKey := msg.Header.Get("api-key")
	icao := msg.Header.Get("icao") // formatted like 7C1234
	returnSubject := msg.Header.Get("subject")
	logger := log.With().
		Str("action", action).
		Str("apiKey", apiKey).
		Str("icao", icao).
		Str("returnSubject", returnSubject).
		Logger()

	var uIcao uint64
	if icao != "" {
		var err error
		uIcao, err = strconv.ParseUint(icao, 16, 24)
		if nil != err {
			// Treating a bad ICAO as no filter would send this client every frame.
			logger.Error().Err(err).Msg("Network Tap Request rejected: bad icao")
			_ = msg.Respond([]byte("Error: bad icao"))
			return
		}
	}
	if returnSubject == "" {
		logger.Error().Msg("Network Tap Request rejected: no subject")
		_ = msg.Respond([]byte("Error: no subject"))
		return
	}

	c := condition{
		subject: returnSubject,
		apiKey:  apiKey,
		icao:    uint32(uIcao),
	}

	switch action {
	case "add":
		if tap.add(c) {
			logger.Info().Msg("Network Tap Request: condition added")
		} else {
			logger.Debug().Msg("Network Tap Request: condition refreshed")
		}
	case "remove":
		if tap.remove(c) {
			logger.Info().Msg("Network Tap Request: condition removed")
		} else {
			logger.Debug().Msg("Network Tap Request: nothing to remove")
		}
	default:
		logger.Warn().Msg("Network Tap Request: unknown action")
	}
	_ = msg.Respond([]byte("Ack"))
}

// add records c, or refreshes it if already present. It reports whether c is new.
func (tap *IngestTap) add(c condition) bool {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	_, exists := tap.lastAdded[c]
	tap.lastAdded[c] = tap.now()
	if !exists {
		tap.publishLocked()
	}
	return !exists
}

// remove deletes c and reports whether it was present.
func (tap *IngestTap) remove(c condition) bool {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	if _, exists := tap.lastAdded[c]; !exists {
		return false
	}
	delete(tap.lastAdded, c)
	tap.publishLocked()
	return true
}

// expire drops conditions not added for tap.expiry, as if their client had sent "remove".
func (tap *IngestTap) expire() {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	now := tap.now()
	changed := false
	for c, added := range tap.lastAdded {
		if now.Sub(added) >= tap.expiry {
			delete(tap.lastAdded, c)
			changed = true
			log.Info().
				Str("apiKey", c.apiKey).
				Str("returnSubject", c.subject).
				Uint32("icao", c.icao).
				Msg("Network Tap condition expired")
		}
	}
	if changed {
		tap.publishLocked()
	}
}

func (tap *IngestTap) expireLoop() {
	defer tap.stopWg.Done()
	ticker := time.NewTicker(tapExpiryCheck)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			tap.expire()
		case <-tap.stop:
			return
		}
	}
}

// publishLocked builds a new snapshot from lastAdded and swaps it in. Callers hold tap.mu.
func (tap *IngestTap) publishLocked() {
	set := &conditionSet{}
	for c := range tap.lastAdded {
		if c.icao != 0 {
			set.withIcao = true
		}
		if c.apiKey == "" {
			set.anyTag = append(set.anyTag, c)
			continue
		}
		if nil == set.byTag {
			set.byTag = make(map[string][]condition)
		}
		set.byTag[c.apiKey] = append(set.byTag[c.apiKey], c)
	}
	tap.conds.Store(set)
}

func (s *conditionSet) empty() bool {
	return len(s.anyTag) == 0 && len(s.byTag) == 0
}

func (s *conditionSet) matches(tag string, icao uint32) bool {
	for _, c := range s.anyTag {
		if c.matchIcao(icao) {
			return true
		}
	}
	for _, c := range s.byTag[tag] {
		if c.matchIcao(icao) {
			return true
		}
	}
	return false
}

func (c condition) matchIcao(icao uint32) bool {
	return c.icao == 0 || c.icao == icao
}
