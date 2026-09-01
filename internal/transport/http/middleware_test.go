package httptransport

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	platformauthz "github.com/lihongjie0209/microservice-platform-go/authz"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/scheduler-service/internal/auth"
	"github.com/lihongjie0209/scheduler-service/internal/config"
)

func TestRequestID(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID())
	router.POST("/test", func(c *gin.Context) { OK(c, nil) })
	request := httptest.NewRequest(http.MethodPost, "/test", nil)
	request.Header.Set("X-Request-ID", "client-request-1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("X-Request-ID"); got != "client-request-1" {
		t.Fatalf("X-Request-ID = %q", got)
	}
	var response Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.RequestID != "client-request-1" {
		t.Fatalf("request_id = %q", response.RequestID)
	}
}

func TestAuthentication_PSKPrecedesSkipAndJWT(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	const key = "01234567890123456789012345678901"
	service := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}})
	for _, test := range []struct {
		name   string
		header string
		status int
	}{
		{name: "valid PSK", header: "PSK " + key, status: http.StatusOK},
		{name: "PSK route does not become public", status: http.StatusUnauthorized},
		{name: "bearer cannot access PSK route", header: "Bearer invalid", status: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			router := gin.New()
			router.Use(RequestID(), Authentication(service, slog.New(slog.NewTextHandler(io.Discard, nil)), config.Auth{
				SkipHTTPPaths: []string{"/api/v1/external/*"},
				PSK:           config.PSK{Enabled: true, Key: key, HTTPPaths: []string{"/api/v1/external/*"}},
			}))
			router.POST("/api/v1/external/callback", func(c *gin.Context) {
				value, ok := principal.FromContext(c.Request.Context())
				if test.status == http.StatusOK && (!ok || value.ID != "scheduler-service:psk" || value.Type != principal.TypeServiceAccount) {
					c.AbortWithStatus(http.StatusInternalServerError)
					return
				}
				OK(c, nil)
			})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/external/callback", nil)
			request.Header.Set("Authorization", test.header)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d", recorder.Code, test.status)
			}
		})
	}
}

func TestSchedulerHTTPRequirementCoversEveryBusinessRoute(t *testing.T) {
	t.Parallel()
	routes := []string{"/api/v1/scheduler/jobs/create", "/api/v1/scheduler/jobs/update", "/api/v1/scheduler/jobs/delete", "/api/v1/scheduler/jobs/get", "/api/v1/scheduler/jobs/list", "/api/v1/scheduler/jobs/trigger", "/api/v1/scheduler/executions/get", "/api/v1/scheduler/executions/list"}
	for _, route := range routes {
		requirement, ok := schedulerHTTPRequirement(route)
		if !ok || requirement.Resource == "" || requirement.Action == "" || requirement.Scope != platformauthz.ScopeTenant {
			t.Fatalf("route %q requirement = %+v, %v", route, requirement, ok)
		}
	}
}

type authorizerStub struct{ err error }

func (stub authorizerStub) Authorize(context.Context, principal.Principal, platformauthz.Requirement) error {
	return stub.err
}

func TestAuthorizationMapsDenialAndDecisionOutage(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{name: "denied", err: platformauthz.ErrDenied, want: http.StatusForbidden},
		{name: "outage", err: platformauthz.ErrDecisionUnavailable, want: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			router := gin.New()
			router.Use(RequestID(), func(c *gin.Context) {
				c.Request = c.Request.WithContext(principal.WithContext(c.Request.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser}))
				c.Next()
			}, Authorization(true, authorizerStub{err: test.err}, slog.New(slog.NewTextHandler(io.Discard, nil))))
			router.POST("/api/v1/scheduler/jobs/list", func(c *gin.Context) { OK(c, nil) })
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/scheduler/jobs/list", nil))
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d", recorder.Code, test.want)
			}
		})
	}
}

func TestRequireJSON(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID(), RequireJSON())
	router.POST("/test", func(c *gin.Context) { OK(c, nil) })
	request := httptest.NewRequest(http.MethodPost, "/test", io.NopCloser(&oneByteReader{}))
	request.ContentLength = 1
	request.Header.Set("Content-Type", "text/plain")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTimeoutPropagatesCancellation(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := gin.New()
	router.Use(RequestID(), Timeout(time.Millisecond, logger))
	router.POST("/test", func(c *gin.Context) { <-c.Request.Context().Done() })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/test", nil))
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusGatewayTimeout)
	}
}

type oneByteReader struct{}

func (*oneByteReader) Read(buffer []byte) (int, error) { buffer[0] = 'x'; return 1, io.EOF }
