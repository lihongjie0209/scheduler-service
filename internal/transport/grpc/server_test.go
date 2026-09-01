package grpctransport

import (
	"testing"
	"time"

	platformauthz "github.com/lihongjie0209/microservice-platform-go/authz"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	schedulerv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/scheduler/v1"
	"github.com/lihongjie0209/scheduler-service/internal/auth"
	"github.com/lihongjie0209/scheduler-service/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthenticateGRPC_PSKWildcard(t *testing.T) {
	t.Parallel()
	const key = "01234567890123456789012345678901"
	authService := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}})
	cfg := config.Auth{
		SkipGRPCMethods: []string{"/hello.v1.UserService/*"},
		PSK:             config.PSK{Enabled: true, Key: key, GRPCMethods: []string{"/hello.v1.UserService/*"}},
	}
	for _, test := range []struct {
		name   string
		header string
		code   codes.Code
	}{
		{name: "valid", header: "PSK " + key, code: codes.OK},
		{name: "PSK precedes skip", code: codes.Unauthenticated},
		{name: "bearer rejected", header: "Bearer " + key, code: codes.Unauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", test.header))
			authenticated, err := authenticateGRPC(ctx, "/hello.v1.UserService/GetUser", authService, cfg)
			if got := status.Code(err); got != test.code {
				t.Fatalf("status code = %s, want %s", got, test.code)
			}
			if test.code == codes.OK {
				value, ok := principal.FromContext(authenticated)
				if !ok || value.ID != "scheduler-service:psk" || value.Type != principal.TypeServiceAccount {
					t.Fatalf("principal = %#v, %v", value, ok)
				}
			}
		})
	}
}

func TestSchedulerGRPCRequirementCoversEveryBusinessMethod(t *testing.T) {
	t.Parallel()
	resolve := schedulerGRPCRequirement(true)
	methods := []string{schedulerv1.SchedulerService_CreateJob_FullMethodName, schedulerv1.SchedulerService_UpdateJob_FullMethodName, schedulerv1.SchedulerService_DeleteJob_FullMethodName, schedulerv1.SchedulerService_GetJob_FullMethodName, schedulerv1.SchedulerService_ListJobs_FullMethodName, schedulerv1.SchedulerService_TriggerJob_FullMethodName, schedulerv1.SchedulerService_GetExecution_FullMethodName, schedulerv1.SchedulerService_ListExecutions_FullMethodName}
	for _, method := range methods {
		requirement, ok := resolve(method)
		if !ok || requirement.Resource == "" || requirement.Action == "" || requirement.Scope != platformauthz.ScopePlatform {
			t.Fatalf("method %q requirement = %+v, %v", method, requirement, ok)
		}
	}
	if _, ok := schedulerGRPCRequirement(false)(schedulerv1.SchedulerService_ListJobs_FullMethodName); ok {
		t.Fatal("disabled authorization must not enforce")
	}
}

func TestAuthenticateGRPC_JWTInjectsPrincipal(t *testing.T) {
	t.Parallel()
	const key = "01234567890123456789012345678901"
	service := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}, Auth: config.Auth{ClientID: "client", ClientSecret: "secret"}})
	token, err := service.Issue("user-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token))
	ctx, err = authenticateGRPC(ctx, "/hello.v1.UserService/GetUser", service, config.Auth{})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := principal.FromContext(ctx)
	if !ok || value.ID != "user-1" || value.Type != principal.TypeServiceAccount {
		t.Fatalf("principal = %#v, %v", value, ok)
	}
}
