package clickhouse

import (
	"context"
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/url"
	"reflect"
	"strings"
	"time"
)

type (
	Server struct {
		// expecting something like clickhouse://user:pass@127.0.0.1:9000
		connUrl string
		conn    driver.Conn

		connected bool

		log zerolog.Logger
	}
)

var ErrNotConnected = errors.New("not connected to clickhouse server")

func New(url string) (*Server, error) {
	chs := &Server{
		connUrl: url,
		log:     log.With().Str("section", "clickhouse").Logger(),
	}
	var err error
	for i := 0; i < 5; i++ {
		err = chs.Connect()

		if nil == err {
			log.Info().Str("ClickHouse", url).Msg("Connected")
			return chs, nil
		}
	}
	log.Error().Err(err).Str("ClickHouse", url).Msg("Failed to Connect")
	return nil, err
}

func (chs *Server) Connect() error {
	var err error

	urlParts, err := url.Parse(chs.connUrl)
	if nil != err {
		return err
	}
	username := urlParts.User.Username()
	password, _ := urlParts.User.Password()
	database := "plane_watch"
	if "" != urlParts.Path && "/" != urlParts.Path {
		database = urlParts.Path
	}
	urlParts.User = nil
	urlParts.Path = ""

	chs.log.Info().Str("URL", urlParts.String()).Msg("Attempting to connect to")

	chs.conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{urlParts.Host},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		MaxOpenConns: 100,
		MaxIdleConns: 50,
	})
	if nil != err {
		return err
	}

	chs.connected = true
	return nil
}

// Inserts Add a batch of rows into the given clickhouse feed. Every row must
// be a pointer to the same struct type; the insert names that struct's fields
// as its columns, so a table may carry columns the struct does not know
// (they take their defaults) and a column can be added to the table before
// the code that fills it is deployed.
func (chs *Server) Inserts(table string, d []any, max int) error {
	if chs.log.Trace().Enabled() {
		chs.log.Trace().Str("table", table).Interface("data", d).Msg("insert")
	}
	if max <= 0 {
		return nil
	}
	t := time.Now()
	ctx := context.Background()
	batch, err := chs.conn.PrepareBatch(ctx, InsertQuery(table, d[0]))
	if nil != err {
		return err
	}
	for i := 0; i < max; i++ {
		err = batch.AppendStruct(d[i])
		if nil != err {
			chs.log.Error().Err(err).Msg("Did not insert data")
			return err
		}
	}
	defer func() {
		chs.log.Debug().
			TimeDiff("Time Taken", time.Now(), t).
			Str("table", table).
			Int("Num Rows", max).
			Msg("Insert Batch")
	}()

	return batch.Send()
}

func (chs *Server) Select(ctx context.Context, dest any, query string, args ...any) error {
	if chs.log.Trace().Enabled() {
		chs.log.Trace().Str("query", query).Msg("query")
	}
	if !chs.connected {
		chs.log.Error().Msg("Not connected to Clickhouse")
		return ErrNotConnected
	}

	t1 := time.Now()
	defer func() {
		d := time.Since(t1)
		if d > 500*time.Millisecond {
			chs.log.Warn().
				Str("Query", query).
				Dur("Time Taken", d).
				Msg("Time Taken To Run Query")
		}
	}()

	return chs.conn.Select(ctx, dest, query, args...)
}

// InsertQuery builds "INSERT INTO table (col, ...)" from the exported fields
// of row, a struct or pointer to one, named as the driver's AppendStruct
// names them: the `ch` tag when present, else the field name. A `ch:"-"`
// field is left out. A row that is not a struct gives a bare insert.
func InsertQuery(table string, row any) string {
	cols := StructColumns(row)
	if len(cols) == 0 {
		return "INSERT INTO " + table
	}
	return "INSERT INTO " + table + " (" + strings.Join(cols, ", ") + ")"
}

// StructColumns lists the column names AppendStruct would use for row.
func StructColumns(row any) []string {
	if nil == row {
		return nil
	}
	t := reflect.TypeOf(row)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	cols := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name := f.Name
		if tag := f.Tag.Get("ch"); tag != "" {
			if tag == "-" {
				continue
			}
			name = tag
		}
		cols = append(cols, name)
	}
	return cols
}
