package job

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/lihongjie0209/scheduler-service/internal/outbound"
	"google.golang.org/grpc"
)

var ErrUpstreamNotConfigured = errors.New("gRPC upstream is not configured")

type Invoker interface {
	Validate(context.Context, string, string, string) error
	Invoke(context.Context, string, string, string) (string, error)
}

type connectionRegistry interface {
	GRPC(string) (*grpc.ClientConn, bool)
}
type DynamicInvoker struct{ registry connectionRegistry }

func NewDynamicInvoker(registry *outbound.Registry) Invoker {
	return &DynamicInvoker{registry: registry}
}

func (i *DynamicInvoker) Validate(ctx context.Context, upstream, fullMethod, requestJSON string) error {
	_, source, closeReflection, err := i.resolve(ctx, upstream, fullMethod)
	if err != nil {
		return err
	}
	defer closeReflection()
	parser, _, err := grpcurl.RequestParserAndFormatter(grpcurl.FormatJSON, source, strings.NewReader(requestJSON), grpcurl.FormatOptions{})
	if err != nil {
		return fmt.Errorf("create dynamic JSON parser: %w", err)
	}
	descriptor, err := source.FindSymbol(symbolName(fullMethod))
	if err != nil {
		return fmt.Errorf("resolve gRPC method %q: %w", fullMethod, err)
	}
	method, ok := descriptor.(*desc.MethodDescriptor)
	if !ok {
		return fmt.Errorf("gRPC target %q is not a method", fullMethod)
	}
	if method.IsClientStreaming() || method.IsServerStreaming() {
		return fmt.Errorf("scheduled gRPC method %q must be unary", fullMethod)
	}
	request := dynamic.NewMessage(method.GetInputType())
	if err := parser.Next(request); err != nil {
		return fmt.Errorf("decode request JSON for %q: %w", fullMethod, err)
	}
	if err := parser.Next(dynamic.NewMessage(method.GetInputType())); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request JSON for %q must contain exactly one message", fullMethod)
	}
	return nil
}

func (i *DynamicInvoker) Invoke(ctx context.Context, upstream, fullMethod, requestJSON string) (string, error) {
	connection, source, closeReflection, err := i.resolve(ctx, upstream, fullMethod)
	if err != nil {
		return "", err
	}
	defer closeReflection()
	parser, formatter, err := grpcurl.RequestParserAndFormatter(grpcurl.FormatJSON, source, strings.NewReader(requestJSON), grpcurl.FormatOptions{})
	if err != nil {
		return "", fmt.Errorf("create dynamic request codec: %w", err)
	}
	var output bytes.Buffer
	handler := grpcurl.NewDefaultEventHandler(&output, source, formatter, false)
	if err := grpcurl.InvokeRPC(ctx, source, connection, strings.TrimPrefix(fullMethod, "/"), nil, handler, parser.Next); err != nil {
		return "", fmt.Errorf("invoke dynamic gRPC method %q: %w", fullMethod, err)
	}
	if handler.Status != nil && handler.Status.Err() != nil {
		return "", handler.Status.Err()
	}
	if handler.NumResponses != 1 {
		return "", fmt.Errorf("unary gRPC method %q returned %d responses", fullMethod, handler.NumResponses)
	}
	return strings.TrimSpace(output.String()), nil
}

func (i *DynamicInvoker) resolve(ctx context.Context, upstream, fullMethod string) (*grpc.ClientConn, grpcurl.DescriptorSource, func(), error) {
	connection, ok := i.registry.GRPC(strings.TrimSpace(upstream))
	if !ok {
		return nil, nil, func() {}, fmt.Errorf("%w: %s", ErrUpstreamNotConfigured, upstream)
	}
	if !validFullMethod(fullMethod) {
		return nil, nil, func() {}, fmt.Errorf("invalid full gRPC method %q", fullMethod)
	}
	reflectionClient := grpcreflect.NewClientAuto(ctx, connection)
	source := grpcurl.DescriptorSourceFromServer(ctx, reflectionClient)
	descriptor, err := source.FindSymbol(symbolName(fullMethod))
	if err != nil {
		reflectionClient.Reset()
		return nil, nil, func() {}, fmt.Errorf("resolve gRPC method %q through reflection: %w", fullMethod, err)
	}
	method, ok := descriptor.(*desc.MethodDescriptor)
	if !ok || method.IsClientStreaming() || method.IsServerStreaming() {
		reflectionClient.Reset()
		return nil, nil, func() {}, fmt.Errorf("scheduled gRPC target %q must be a unary method", fullMethod)
	}
	return connection, source, reflectionClient.Reset, nil
}

func validFullMethod(value string) bool {
	value = strings.TrimSpace(value)
	parts := strings.Split(strings.TrimPrefix(value, "/"), "/")
	return len(parts) == 2 && strings.Contains(parts[0], ".") && parts[1] != ""
}
func symbolName(fullMethod string) string {
	return strings.Replace(strings.TrimPrefix(strings.TrimSpace(fullMethod), "/"), "/", ".", 1)
}
