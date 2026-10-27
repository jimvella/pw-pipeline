package tracker

import (
	"math"
	"time"

	"kreklow.us/go/go-adsb/adsb"
)

// Navigation (TC29 target state, BDS 4,0) and enhanced surveillance (BDS 5,0
// and 6,0) fields held on a Plane. They are decoded with go-adsb in commb.go;
// this file holds the values and the rules for accepting them.

const (
	// navExpiry is how long a nav or EHS value stays current without being
	// refreshed, measured against the plane's last seen time so that a
	// stale selected altitude does not ride along with a live position.
	navExpiry = 60 * time.Second

	// velocityResolveWindow is how old a TC19 velocity may be and still
	// decide between an ambiguous BDS 5,0 / 6,0 reply.
	velocityResolveWindow = 10 * time.Second

	// selectedAltitudeTolerance is the difference allowed between a BDS 4,0
	// selected altitude (16 ft steps) and a TC29 one (32 ft steps) for the
	// two to count as the same value.
	selectedAltitudeTolerance = 32
)

type (
	// NavModes are the autopilot modes reported by TC29 (and, for the three
	// it carries, BDS 4,0).
	NavModes struct {
		Autopilot bool
		VNAV      bool
		LNAV      bool
		AltHold   bool
		Approach  bool
		TCAS      bool
	}

	// NavState is what the aircraft's flight control system is set to.
	// A nil pointer means the aircraft has not reported the value.
	NavState struct {
		SelectedAltitudeMCP *int     // ft, MCP/FCU window
		SelectedAltitudeFMS *int     // ft, FMS
		QNH                 *float64 // hPa
		SelectedHeading     *float64 // degrees
		Modes               *NavModes
	}

	// EhsState is the enhanced surveillance (BDS 5,0 and 6,0) picture of the
	// aircraft. A nil pointer means the aircraft has not reported the value.
	EhsState struct {
		IAS                  *int     // kt
		TAS                  *int     // kt
		Mach                 *float64 // ratio
		Roll                 *float64 // degrees, right wing down positive
		MagneticHeading      *float64 // degrees
		TrackRate            *float64 // degrees per second
		BaroVerticalRate     *int     // ft/min
		InertialVerticalRate *int     // ft/min
	}

	// bds40Reading is an inferred BDS 4,0 that has been seen but not yet
	// accepted, kept for the seen-twice rule.
	bds40Reading struct {
		mcp, fms *int
		qnh      *float64
		ts       time.Time
	}

	navTracking struct {
		state NavState
		ts    time.Time // last accepted update

		tc29TS     time.Time // last TC29 with a selected altitude
		tc29SelAlt int
		tc29FMS    bool

		last40 *bds40Reading
	}

	ehsTracking struct {
		state EhsState
		ts    time.Time
	}
)

// Strings gives the modes in readsb's nav_modes names.
func (m NavModes) Strings() []string {
	var out []string
	if m.Autopilot {
		out = append(out, "autopilot")
	}
	if m.VNAV {
		out = append(out, "vnav")
	}
	if m.LNAV {
		out = append(out, "lnav")
	}
	if m.AltHold {
		out = append(out, "althold")
	}
	if m.Approach {
		out = append(out, "approach")
	}
	if m.TCAS {
		out = append(out, "tcas")
	}
	return out
}

func (n NavState) isEmpty() bool {
	return n.SelectedAltitudeMCP == nil && n.SelectedAltitudeFMS == nil && n.QNH == nil &&
		n.SelectedHeading == nil && n.Modes == nil
}

func (e EhsState) isEmpty() bool {
	return e.IAS == nil && e.TAS == nil && e.Mach == nil && e.Roll == nil && e.MagneticHeading == nil &&
		e.TrackRate == nil && e.BaroVerticalRate == nil && e.InertialVerticalRate == nil
}

// Nav returns the navigation state and whether it is current: something has
// been reported and it was refreshed within navExpiry of the last message.
func (p *Plane) Nav() (NavState, bool) {
	p.rwLock.RLock()
	defer p.rwLock.RUnlock()
	return p.nav.state, p.navCurrentLocked()
}

// EHS returns the enhanced surveillance state and whether it is current.
func (p *Plane) EHS() (EhsState, bool) {
	p.rwLock.RLock()
	defer p.rwLock.RUnlock()
	return p.ehs.state, p.ehsCurrentLocked()
}

func (p *Plane) NavUpdatedAt() time.Time {
	p.rwLock.RLock()
	defer p.rwLock.RUnlock()
	return p.nav.ts
}

func (p *Plane) EhsUpdatedAt() time.Time {
	p.rwLock.RLock()
	defer p.rwLock.RUnlock()
	return p.ehs.ts
}

func (p *Plane) navCurrentLocked() bool {
	return !p.nav.state.isEmpty() && !p.nav.ts.IsZero() && p.lastSeen.Sub(p.nav.ts) <= navExpiry
}

func (p *Plane) ehsCurrentLocked() bool {
	return !p.ehs.state.isEmpty() && !p.ehs.ts.IsZero() && p.lastSeen.Sub(p.ehs.ts) <= navExpiry
}

// setTargetState takes a TC29 subtype 1 message as it comes. A field the
// message does not carry (nil) is not a change and leaves the old value.
func (p *Plane) setTargetState(ts *adsb.TargetState, t time.Time) bool {
	if ts == nil || t.IsZero() {
		return false
	}
	p.rwLock.Lock()
	defer p.rwLock.Unlock()
	var changed bool
	if ts.SelectedAltitude != nil {
		if ts.SelectedAltitudeFMS {
			changed = setIntPtr(&p.nav.state.SelectedAltitudeFMS, *ts.SelectedAltitude) || changed
		} else {
			changed = setIntPtr(&p.nav.state.SelectedAltitudeMCP, *ts.SelectedAltitude) || changed
		}
		p.nav.tc29TS = t
		p.nav.tc29SelAlt = *ts.SelectedAltitude
		p.nav.tc29FMS = ts.SelectedAltitudeFMS
	}
	if ts.BarometricPressureSetting != nil {
		changed = setFloatPtr(&p.nav.state.QNH, *ts.BarometricPressureSetting) || changed
	}
	if ts.SelectedHeading != nil {
		changed = setFloatPtr(&p.nav.state.SelectedHeading, *ts.SelectedHeading) || changed
	}
	if ts.ModeBitsValid {
		modes := NavModes{
			Autopilot: ts.AutopilotEngaged,
			VNAV:      ts.VNAVEngaged,
			LNAV:      ts.LNAVEngaged,
			AltHold:   ts.AltitudeHold,
			Approach:  ts.ApproachMode,
			TCAS:      ts.TCASOperational,
		}
		changed = setModes(&p.nav.state.Modes, modes) || changed
	}
	p.nav.ts = t
	return changed
}

// setBds40 takes an inferred BDS 4,0 (selected vertical intention) reply.
// Inference can be wrong, and a wrong register read as 4,0 would become a
// false selected altitude, so a reading is accepted only when it agrees
// with a TC29 selected altitude seen in the last navExpiry, or when the
// same values have been seen twice. While TC29 is current the reply adds
// only what TC29 lacks (the FMS altitude); without TC29, as on ADS-B
// version 0 aircraft, it supplies everything.
func (p *Plane) setBds40(svi *adsb.SelectedVerticalIntention, t time.Time) bool {
	if svi == nil || t.IsZero() {
		return false
	}
	if svi.MCPSelectedAltitude == nil && svi.FMSSelectedAltitude == nil && svi.BarometricPressureSetting == nil {
		return false
	}
	p.rwLock.Lock()
	defer p.rwLock.Unlock()

	reading := &bds40Reading{mcp: svi.MCPSelectedAltitude, fms: svi.FMSSelectedAltitude, qnh: svi.BarometricPressureSetting, ts: t}
	tc29Current := !p.nav.tc29TS.IsZero() && t.Sub(p.nav.tc29TS) <= navExpiry && t.Sub(p.nav.tc29TS) >= -navExpiry
	accepted := false
	if tc29Current {
		if p.nav.tc29FMS {
			accepted = svi.FMSSelectedAltitude != nil && near(*svi.FMSSelectedAltitude, p.nav.tc29SelAlt, selectedAltitudeTolerance)
		} else {
			accepted = svi.MCPSelectedAltitude != nil && near(*svi.MCPSelectedAltitude, p.nav.tc29SelAlt, selectedAltitudeTolerance)
		}
	}
	if !accepted && p.nav.last40 != nil && t.Sub(p.nav.last40.ts) <= navExpiry && p.nav.last40.sameValues(reading) {
		accepted = true
	}
	p.nav.last40 = reading
	if !accepted {
		return false
	}

	var changed bool
	if svi.FMSSelectedAltitude != nil {
		changed = setIntPtr(&p.nav.state.SelectedAltitudeFMS, *svi.FMSSelectedAltitude) || changed
	}
	if !tc29Current {
		if svi.MCPSelectedAltitude != nil {
			changed = setIntPtr(&p.nav.state.SelectedAltitudeMCP, *svi.MCPSelectedAltitude) || changed
		}
		if svi.BarometricPressureSetting != nil {
			changed = setFloatPtr(&p.nav.state.QNH, *svi.BarometricPressureSetting) || changed
		}
		if svi.ModeBitsValid {
			modes := NavModes{VNAV: svi.VNAVMode, AltHold: svi.AltitudeHold, Approach: svi.ApproachMode}
			if p.nav.state.Modes != nil {
				modes.Autopilot = p.nav.state.Modes.Autopilot
				modes.LNAV = p.nav.state.Modes.LNAV
				modes.TCAS = p.nav.state.Modes.TCAS
			}
			changed = setModes(&p.nav.state.Modes, modes) || changed
		}
	}
	p.nav.ts = t
	return changed
}

func (r *bds40Reading) sameValues(o *bds40Reading) bool {
	return eqIntPtr(r.mcp, o.mcp) && eqIntPtr(r.fms, o.fms) && eqFloatPtr(r.qnh, o.qnh)
}

// setBds50 takes a BDS 5,0 (track and turn) reply that is known to be one.
func (p *Plane) setBds50(tt *adsb.TrackAndTurn, t time.Time) bool {
	if tt == nil || t.IsZero() {
		return false
	}
	p.rwLock.Lock()
	defer p.rwLock.Unlock()
	var changed bool
	if tt.RollAngle != nil {
		changed = setFloatPtr(&p.ehs.state.Roll, *tt.RollAngle) || changed
	}
	if tt.TrackAngleRate != nil {
		changed = setFloatPtr(&p.ehs.state.TrackRate, *tt.TrackAngleRate) || changed
	}
	if tt.TrueAirspeed != nil {
		changed = setIntPtr(&p.ehs.state.TAS, int(math.Round(*tt.TrueAirspeed))) || changed
	}
	// Ground speed and true track duplicate TC19 and are not kept.
	p.ehs.ts = t
	return changed
}

// setBds60 takes a BDS 6,0 (heading and speed) reply that is known to be one.
func (p *Plane) setBds60(hs *adsb.HeadingAndSpeed, t time.Time) bool {
	if hs == nil || t.IsZero() {
		return false
	}
	p.rwLock.Lock()
	defer p.rwLock.Unlock()
	var changed bool
	if hs.MagneticHeading != nil {
		changed = setFloatPtr(&p.ehs.state.MagneticHeading, *hs.MagneticHeading) || changed
	}
	if hs.IndicatedAirspeed != nil {
		changed = setIntPtr(&p.ehs.state.IAS, int(math.Round(*hs.IndicatedAirspeed))) || changed
	}
	if hs.Mach != nil {
		changed = setFloatPtr(&p.ehs.state.Mach, *hs.Mach) || changed
	}
	if hs.BarometricAltitudeRate != nil {
		changed = setIntPtr(&p.ehs.state.BaroVerticalRate, *hs.BarometricAltitudeRate) || changed
	}
	if hs.InertialVerticalVelocity != nil {
		changed = setIntPtr(&p.ehs.state.InertialVerticalRate, *hs.InertialVerticalVelocity) || changed
	}
	p.ehs.ts = t
	return changed
}

func setIntPtr(dst **int, v int) bool {
	if *dst != nil && **dst == v {
		return false
	}
	*dst = &v
	return true
}

func setFloatPtr(dst **float64, v float64) bool {
	if *dst != nil && **dst == v {
		return false
	}
	*dst = &v
	return true
}

func setModes(dst **NavModes, v NavModes) bool {
	if *dst != nil && **dst == v {
		return false
	}
	*dst = &v
	return true
}

func eqIntPtr(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func eqFloatPtr(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func near(a, b, tolerance int) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= tolerance
}
