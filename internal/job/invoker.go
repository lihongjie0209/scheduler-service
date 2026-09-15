package job

import (
	"context"
	"errors"

	"github.com/lihongjie0209/microservice-platform-go/dynamicgrpc"
	"github.com/lihongjie0209/scheduler-service/internal/outbound"
	"google.golang.org/grpc"
)

var ErrUpstreamNotConfigured = dynamicgrpc.ErrUpstreamNotConfigured

type Invoker interface {
	Validate(context.Context, string, string, string) error
	Invoke(context.Context, string, string, string) (string, error)
}

type connectionRegistry interface {
	GRPC(string) (*grpc.ClientConn, bool)
}

func NewDynamicInvoker(registry *outbound.Registry) (Invoker, error) {
	if registry == nil {
		return nil, errors.New("outbound registry is required")
	}
	return newDynamicInvoker(registry)
}

func newDynamicInvoker(registry connectionRegistry) (Invoker, error) {
	return dynamicgrpc.New(registry)
}
