package clickhouse

import (
	"reflect"
	"testing"
	"time"
)

type sampleRow struct {
	Icao     string
	Altitude int32
	Renamed  float64 `ch:"Velocity"`
	Skipped  string  `ch:"-"`
	LastMsg  time.Time
	hidden   int
}

func TestStructColumns(t *testing.T) {
	want := []string{"Icao", "Altitude", "Velocity", "LastMsg"}
	if got := StructColumns(&sampleRow{}); !reflect.DeepEqual(got, want) {
		t.Errorf("StructColumns(pointer) = %v, want %v", got, want)
	}
	if got := StructColumns(sampleRow{}); !reflect.DeepEqual(got, want) {
		t.Errorf("StructColumns(value) = %v, want %v", got, want)
	}
	if got := StructColumns(nil); got != nil {
		t.Errorf("StructColumns(nil) = %v, want nil", got)
	}
	if got := StructColumns(42); got != nil {
		t.Errorf("StructColumns(int) = %v, want nil", got)
	}
}

func TestInsertQuery(t *testing.T) {
	want := "INSERT INTO location_updates_low (Icao, Altitude, Velocity, LastMsg)"
	if got := InsertQuery("location_updates_low", &sampleRow{}); got != want {
		t.Errorf("InsertQuery = %q, want %q", got, want)
	}
	if got := InsertQuery("t", "not a struct"); got != "INSERT INTO t" {
		t.Errorf("InsertQuery(non-struct) = %q", got)
	}
}
