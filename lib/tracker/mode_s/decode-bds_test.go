package mode_s

import (
	"math"
	"testing"
	"time"
)

func Test_inferCommBMessageType(t *testing.T) {
	type args struct {
		mb []byte
	}
	// Logic for the args. If we need to specify bits for detection they are in 0b binary notation. 0's and 0xFF's are junk data
	tests := []struct {
		name    string
		args    args
		want    byte
		want1   byte
		wantErr bool
	}{
		{
			name:    "Correct Length",
			args:    args{mb: []byte{}},
			want:    0,
			want1:   0,
			wantErr: true,
		},
		{
			name:    "Infer BDS 1.0",
			args:    args{mb: []byte{0b0001_0000, 0b1000_0011, 0, 0xFF, 0, 0xFF, 0}},
			want:    1,
			want1:   0,
			wantErr: false,
		},
		{
			name:    "Infer BDS 1.7",
			args:    args{mb: []byte{0b0000_0010, 0xFF, 0xFF, 0b1111_0000, 0b0, 0b0, 0b0}},
			want:    1,
			want1:   7,
			wantErr: false,
		},
		{
			name:    "Infer BDS 2.0",
			args:    args{mb: []byte{0b0010_0000, 0b0100_1100, 0b1001_0000, 0b0111_0010, 0b1100_1011, 0b0100_1000, 0b0010_0000}},
			want:    2,
			want1:   0,
			wantErr: false,
		},
		{
			name:    "Infer BDS 3.0 1",
			args:    args{mb: []byte{0b0011_0000, 0b1111_1110, 0b0011_1100, 0b0000_1000, 0xFF, 0xFF, 0xFF}},
			want:    3,
			want1:   0,
			wantErr: false,
		},
		{
			name:    "Infer BDS 3.0 2",
			args:    args{mb: []byte{0b0011_0000, 0b1111_1110, 0b0100_1100, 0b0000_0100, 0xFF, 0xFF, 0xFF}},
			want:    3,
			want1:   0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := inferCommBMessageType(tt.args.mb)
			if (err != nil) != tt.wantErr {
				t.Errorf("inferCommBMessageType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("inferCommBMessageType() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("inferCommBMessageType() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

// Real Comm-B replies heard near Perth on 2026-10-22, with the values
// pyModeS 3.6 decodes from them.

func near(t *testing.T, name string, got *float64, want float64, tol float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > tol {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func decodeOrFail(t *testing.T, msg string) *Frame {
	t.Helper()
	f, err := DecodeString(msg, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestBds40(t *testing.T) {
	f := decodeOrFail(t, "a8000489aaf80030a400003ac636") // DF21, CT202
	if f.BdsMessageType() != BdsEhsSelVertIntent {
		t.Fatalf("BDS %s", f.BdsMessageType())
	}
	ts := f.TargetState()
	if ts == nil || ts.Source != TargetStateFromCommB {
		t.Fatalf("target state %+v", ts)
	}
	if ts.SelectedAltitudeMcp == nil || *ts.SelectedAltitudeMcp != 22000 {
		t.Errorf("MCP altitude %v, want 22000", ts.SelectedAltitudeMcp)
	}
	near(t, "QNH", ts.BaroSetting, 1013.0, 0.01)
	if f.SquawkIdentity() != 4224 {
		t.Errorf("squawk %d", f.SquawkIdentity())
	}
}

func TestBds50(t *testing.T) {
	f := decodeOrFail(t, "a8000489fedf691e7fe483dda1f8") // DF21
	if f.BdsMessageType() != BdsEhsTrackTurnReport {
		t.Fatalf("BDS %s", f.BdsMessageType())
	}
	b := f.CommB().B50
	near(t, "roll", b.Roll, -1.7578125, 1e-9)
	near(t, "track", b.TrueTrack, 346.640625, 1e-9)
	near(t, "ground speed", b.GroundSpeed, 242, 0)
	near(t, "track rate", b.TrackRate, -0.125, 1e-9)
	near(t, "TAS", b.TrueAirspeed, 262, 0)

	f = decodeOrFail(t, "a00000bf001abb0980000026671f") // DF20, only track and ground speed
	b = f.CommB().B50
	near(t, "track", b.TrueTrack, 241.34765625, 1e-9)
	near(t, "ground speed", b.GroundSpeed, 76, 0)
	if b.Roll != nil || b.TrackRate != nil || b.TrueAirspeed != nil {
		t.Errorf("unexpected values %+v", b)
	}
	if alt, _ := f.Altitude(); alt != 575 {
		t.Errorf("header altitude %d", alt)
	}
}

func TestBds60(t *testing.T) {
	f := decodeOrFail(t, "a000011996c8d10a3fb0003bac25") // DF20, WLQ (MLAT only)
	if f.BdsMessageType() != BdsEhsHeadingSpeed {
		t.Fatalf("BDS %s", f.BdsMessageType())
	}
	b := f.CommB().B60
	near(t, "heading", b.MagneticHeading, 63.984375, 1e-9)
	near(t, "IAS", b.IndicatedAirspeed, 104, 0)
	near(t, "Mach", b.Mach, 0.16, 1e-9)
	near(t, "baro vertical rate", b.BaroVerticalRate, -320, 0)
	if b.InertialVerticalRate != nil {
		t.Errorf("inertial vertical rate %v", *b.InertialVerticalRate)
	}
}

// Both 5,0 and 6,0 pass their checks; the aircraft's known state decides.
func TestResolveCommB50Or60(t *testing.T) {
	// DF20 from CT202 at 13,050 ft. Read as 5,0: 202 kt, track 217.
	// Read as 6,0: heading 339, IAS 211, Mach 0.404.
	f := decodeOrFail(t, "a00008b2f899a719617c32cec902")
	c := f.CommB()
	if c == nil || len(c.Candidates) != 2 || f.BdsMessageType() != "0.0" {
		t.Fatalf("want two candidates and no choice, got %+v / %s", c, f.BdsMessageType())
	}
	// CT202 was flying track 347 at 240 kt (its own ADS-B velocity).
	actual := KnownState{GroundSpeed: 240, Track: 347, HasVelocity: true, AltitudeFt: 13050, HasAltitude: true}
	if got := ResolveCommB(c, actual); got != BdsEhsHeadingSpeed {
		t.Errorf("with the real state: %q, want 6.0", got)
	}
	// Had it been flying 202 kt on 217, the 5,0 reading would fit.
	other := KnownState{GroundSpeed: 202, Track: 217, HasVelocity: true}
	if got := ResolveCommB(c, other); got != BdsEhsTrackTurnReport {
		t.Errorf("with a 5,0-like state: %q, want 5.0", got)
	}
	if got := ResolveCommB(c, KnownState{}); got != "" {
		t.Errorf("with no state: %q, want undecided", got)
	}
}

// A reply that reads as BDS 4,0 with a selected altitude of 40,992 ft from an
// aircraft at 775 ft, and also as 6,0. It must not become an alt bug.
func TestCommBAmbiguous40(t *testing.T) {
	f := decodeOrFail(t, "a0000117d0100000000000b35c30")
	if f.TargetState() != nil {
		t.Errorf("ambiguous reply produced a target state %+v", f.TargetState())
	}
	c := f.CommB()
	if c == nil || len(c.Candidates) != 2 || c.B40 == nil || c.B60 == nil {
		t.Fatalf("candidates %+v", c)
	}
	k := KnownState{GroundSpeed: 120, Track: 200, HasVelocity: true, AltitudeFt: 775, HasAltitude: true}
	if got := ResolveCommB(c, k); got != "" {
		t.Errorf("resolved to %q, want undecided", got)
	}
}

func TestBds50RollLimit(t *testing.T) {
	mb := make([]byte, 7)
	setBits(mb, 1, 1, 1)
	setBits(mb, 3, 11, 228) // 40.1 degrees
	if decodeBds50(mb) != nil {
		t.Error("40 degree roll accepted as 5,0")
	}
	setBits(mb, 3, 11, 171) // 30.1 degrees
	if b := decodeBds50(mb); b == nil || math.Abs(*b.Roll-30.05859375) > 1e-9 {
		t.Errorf("30 degree roll: %+v", b)
	}
}

func TestIsaConversions(t *testing.T) {
	if a := speedOfSound(0); math.Abs(a-661.5) > 0.5 {
		t.Errorf("sea level speed of sound %v kt", a)
	}
	// From the BDS 6,0 reading above: IAS 211 at Mach 0.404, 13,050 ft.
	if cas := machToCas(0.404, 13050); math.Abs(cas-211) > 5 {
		t.Errorf("Mach 0.404 at 13,050 ft = %v kt CAS, want about 211", cas)
	}
	if tas := casToTas(machToCas(0.78, 35000), 35000); math.Abs(tas-0.78*speedOfSound(35000)) > 0.5 {
		t.Errorf("CAS/TAS round trip at FL350: %v", tas)
	}
}
