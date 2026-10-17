// Command gochain is the Go side of the Python SDK's call-chain test over mTLS (P4.19, P8.5, P8.6): ROLE=front
// serves POST /api/call, which calls NEXT_ID's /demo.Echo/Say on its system surface and answers the error it got;
// ROLE=back serves /demo.Echo/Say on its system surface, always SERVICE_BUSY, and GET /api/count. Both allow only
// CALLER_ID to call their system methods.
package main

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/launcher"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const sayMethod = "/demo.Echo/Say"

type sayServer interface {
	Say(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
}

var desc = grpc.ServiceDesc{ServiceName: "demo.Echo", HandlerType: (*sayServer)(nil),
	Methods: []grpc.MethodDesc{{MethodName: "Say", Handler: func(srv any, ctx context.Context, dec func(any) error,
		ic grpc.UnaryServerInterceptor) (any, error) {
		in := new(wrapperspb.StringValue)
		if err := dec(in); err != nil {
			return nil, err
		}
		h := func(ctx context.Context, req any) (any, error) { return srv.(sayServer).Say(ctx, req.(*wrapperspb.StringValue)) }
		if ic == nil {
			return h(ctx, in)
		}
		return ic(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: sayMethod}, h)
	}}}}

type module struct {
	rt     *runtime.Runtime
	mu     sync.Mutex
	count  int
	lastID string
	caller string
}

func (m *module) Say(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	m.mu.Lock()
	m.count++
	m.lastID = reqctx.RequestID(ctx)
	m.caller, _ = grpcx.Caller(ctx)
	m.mu.Unlock()
	return nil, errs.New(errs.ReasonServiceBusy, "the back end is busy")
}

func (m *module) Routes(r *httpx.Routes) error {
	r.Add(httpx.Route{Method: "GET", Path: "/api/count", Public: true, Handle: func(c *httpx.Context) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		return c.JSON(200, map[string]string{"say": strconv.Itoa(m.count), "request_id": m.lastID, "caller": m.caller})
	}})
	r.Add(httpx.Route{Method: "POST", Path: "/api/call", Public: true, Handle: func(c *httpx.Context) error {
		conn, ok := m.rt.GRPC(os.Getenv("NEXT_ID"))
		if !ok {
			return errors.New("NEXT_ID has no grpc endpoint")
		}
		ctx, cancel := context.WithTimeout(c.Ctx(), 5*time.Second)
		defer cancel()
		err := conn.Invoke(ctx, sayMethod, wrapperspb.String("hi"), new(wrapperspb.StringValue), calls.Idempotent())
		var e *errs.Error
		if !errors.As(err, &e) {
			return c.JSON(200, map[string]string{"error": "none"})
		}
		return c.JSON(200, map[string]string{"reason": e.Reason, "dependency": e.Metadata["dependency"]})
	}})
	return nil
}

func (m *module) Services(s *grpcx.Services) error {
	s.Register(&desc, m, map[string]grpcx.Method{"Say": {Operation: "demo.chain.say", Callers: []string{os.Getenv("CALLER_ID")}}})
	return nil
}

func main() {
	port, _ := strconv.Atoi(os.Getenv("GRPC_PORT"))
	launcher.Main(component.Definition{ID: os.Getenv("MEMBER_ID"), Version: "3.0.0", GRPCPort: port,
		Authz: &authz.Definitions{Component: os.Getenv("MEMBER_ID"),
			Resources: []authz.Resource{{Type: "demo.chain", ViewOperation: "demo.chain.read",
				Dimensions: map[string]authz.Dimension{}, Fields: map[string]authz.Field{}}},
			Operations: []authz.Operation{{Key: "demo.chain.read", ResourceType: "demo.chain", Purpose: "read", RiskClass: "low"},
				{Key: "demo.chain.say", ResourceType: "demo.chain", Purpose: "action", RiskClass: "low"}}},
		New: func(rt *runtime.Runtime) (component.Module, error) { return &module{rt: rt}, nil }})
}
