package job

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
)

type fakeConnections struct{ connection *grpc.ClientConn }

func (f fakeConnections) GRPC(name string) (*grpc.ClientConn, bool) {
	return f.connection, name == "health"
}

func TestDynamicInvoker_ReflectionJSONInvocation(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	reflection.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	invoker := &DynamicInvoker{registry: fakeConnections{connection: connection}}
	if err := invoker.Validate(t.Context(), "health", "/grpc.health.v1.Health/Check", `{"service":""}`); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	response, err := invoker.Invoke(t.Context(), "health", "/grpc.health.v1.Health/Check", `{"service":""}`)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if !strings.Contains(response, `"status": "SERVING"`) {
		t.Fatalf("response = %s", response)
	}
}

func TestDynamicInvoker_RejectsInvalidTargets(t *testing.T) {
	t.Parallel()
	invoker := &DynamicInvoker{registry: fakeConnections{}}
	for _, test := range []struct{ name, upstream, method string }{{name: "unknown upstream", upstream: "missing", method: "/grpc.health.v1.Health/Check"}, {name: "invalid method", upstream: "health", method: "Health"}} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := invoker.Invoke(t.Context(), test.upstream, test.method, `{}`); err == nil {
				t.Fatal("Invoke() error = nil")
			}
		})
	}
}
