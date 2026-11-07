// Command goevents is the Go side of the Python SDK's event interoperability test: ROLE=producer serves
// POST /api/things[?big=1] emitting demo.things events; ROLE=consumer subscribes and serves GET /api/seen.
package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing/fstest"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/launcher"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
)

var things = events.Stream{Name: "demo.things", Partitions: 2}

type producer struct{ rt *runtime.Runtime }

func (p *producer) Routes(r *httpx.Routes) error {
	r.Add(httpx.Route{Method: "POST", Path: "/api/things", Public: true, Handle: func(c *httpx.Context) error {
		id := ids.New().String()
		data := map[string]string{"id": id, "from": "go"}
		if c.Request().URL.Query().Get("big") != "" {
			data["blob"] = strings.Repeat("g", events.MaxPayload+1000)
		}
		err := p.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			_, err := p.rt.Events.Emit(ctx, tx, things.Name, events.Event{Type: "demo.things.created.v1", RecordID: id,
				Version: 1, Projection: events.Full, Data: data})
			return err
		})
		if err != nil {
			return err
		}
		return c.JSON(201, map[string]string{"id": id})
	}})
	return nil
}

type consumer struct{ rt *runtime.Runtime }

func (c *consumer) Routes(r *httpx.Routes) error {
	r.Add(httpx.Route{Method: "GET", Path: "/api/seen", Public: true, Handle: func(x *httpx.Context) error {
		var n, big int
		err := c.rt.DB.Tx(x.Ctx(), func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT count(*), count(*) FILTER (WHERE size > 65536) FROM seen").Scan(&n, &big)
		})
		if err != nil {
			return err
		}
		return x.JSON(200, map[string]int{"count": n, "big": big})
	}})
	return nil
}

func (c *consumer) Subscriptions(s *events.Subscriptions) error {
	s.Add(events.Sub{Name: "things", Streams: []string{things.Name}, Types: []string{"demo.things.created.v1"},
		Handle: func(ctx context.Context, tx db.Tx, e events.Received) error {
			var data map[string]string
			if err := e.Decode(&data); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO seen (event_id, size) VALUES ($1, $2)", e.ID, len(data["blob"]))
			return err
		}})
	return nil
}

func main() {
	id := os.Getenv("MEMBER_ID")
	// a member with a database answers a full recovery's checks on its system surface (be-sdk-go alpha.117, P14.1)
	port, _ := strconv.Atoi(os.Getenv("GRPC_PORT"))
	if os.Getenv("ROLE") == "producer" {
		launcher.Main(component.Definition{ID: id, Version: "3.0.0", Streams: []events.Stream{things}, GRPCPort: port,
			Recovery: "demo.things.recover", Resend: "demo.things.resend",
			Authz:      sectest.Defs(id, "demo.things", "demo.things.read", "demo.things.resend", "demo.things.recover"),
			Migrations: fstest.MapFS{"0001_none.up.sql": {Data: []byte("CREATE TABLE marker (id int);")}},
			New:        func(rt *runtime.Runtime) (component.Module, error) { return &producer{rt: rt}, nil }})
	}
	launcher.Main(component.Definition{ID: id, Version: "3.0.0", Subscribes: true, GRPCPort: port,
		Recovery: "demo.seen.recover", Authz: sectest.Defs(id, "demo.seen", "demo.seen.read", "demo.seen.recover"),
		Migrations: fstest.MapFS{"0001_seen.up.sql": {Data: []byte("CREATE TABLE seen (event_id uuid PRIMARY KEY, size int NOT NULL);")}},
		New:        func(rt *runtime.Runtime) (component.Module, error) { return &consumer{rt: rt}, nil }})
}
