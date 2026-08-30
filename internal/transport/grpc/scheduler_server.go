package grpctransport

import (
	"context"
	"errors"

	schedulerv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/scheduler/v1"
	"github.com/lihongjie0209/scheduler-service/internal/apperror"
	"github.com/lihongjie0209/scheduler-service/internal/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type schedulerServer struct {
	schedulerv1.UnimplementedSchedulerServiceServer
	jobs *job.Service
}

func newSchedulerServer(jobs *job.Service) schedulerv1.SchedulerServiceServer {
	return &schedulerServer{jobs: jobs}
}

func (s *schedulerServer) CreateJob(ctx context.Context, request *schedulerv1.CreateJobRequest) (*schedulerv1.CreateJobResponse, error) {
	value, err := s.jobs.Create(ctx, protoInput(request.GetName(), request.GetCronExpression(), request.GetTimezone(), request.GetUpstream(), request.GetFullMethod(), request.GetRequestJson(), request.GetTimeoutMilliseconds(), request.GetEnabled()))
	if err != nil {
		return nil, grpcError(err)
	}
	return &schedulerv1.CreateJobResponse{Job: protoJob(value)}, nil
}
func (s *schedulerServer) UpdateJob(ctx context.Context, request *schedulerv1.UpdateJobRequest) (*schedulerv1.UpdateJobResponse, error) {
	value, err := s.jobs.Update(ctx, request.GetId(), protoInput(request.GetName(), request.GetCronExpression(), request.GetTimezone(), request.GetUpstream(), request.GetFullMethod(), request.GetRequestJson(), request.GetTimeoutMilliseconds(), request.GetEnabled()), request.GetVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &schedulerv1.UpdateJobResponse{Job: protoJob(value)}, nil
}
func (s *schedulerServer) DeleteJob(ctx context.Context, request *schedulerv1.DeleteJobRequest) (*schedulerv1.DeleteJobResponse, error) {
	if err := s.jobs.Delete(ctx, request.GetId(), request.GetVersion()); err != nil {
		return nil, grpcError(err)
	}
	return &schedulerv1.DeleteJobResponse{}, nil
}
func (s *schedulerServer) GetJob(ctx context.Context, request *schedulerv1.GetJobRequest) (*schedulerv1.GetJobResponse, error) {
	value, err := s.jobs.Get(ctx, request.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return &schedulerv1.GetJobResponse{Job: protoJob(value)}, nil
}
func (s *schedulerServer) ListJobs(ctx context.Context, request *schedulerv1.ListJobsRequest) (*schedulerv1.ListJobsResponse, error) {
	page, err := s.jobs.List(ctx, request.GetStatus(), int(request.GetPage()), int(request.GetPageSize()))
	if err != nil {
		return nil, grpcError(err)
	}
	items := make([]*schedulerv1.Job, 0, len(page.Items))
	for _, value := range page.Items {
		items = append(items, protoJob(value))
	}
	return &schedulerv1.ListJobsResponse{Items: items, Total: page.Total, Page: int32(page.Page), PageSize: int32(page.PageSize)}, nil
}
func (s *schedulerServer) TriggerJob(ctx context.Context, request *schedulerv1.TriggerJobRequest) (*schedulerv1.TriggerJobResponse, error) {
	value, err := s.jobs.Trigger(ctx, request.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return &schedulerv1.TriggerJobResponse{Execution: protoExecution(value)}, nil
}
func (s *schedulerServer) GetExecution(ctx context.Context, request *schedulerv1.GetExecutionRequest) (*schedulerv1.GetExecutionResponse, error) {
	value, err := s.jobs.GetExecution(ctx, request.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return &schedulerv1.GetExecutionResponse{Execution: protoExecution(value)}, nil
}
func (s *schedulerServer) ListExecutions(ctx context.Context, request *schedulerv1.ListExecutionsRequest) (*schedulerv1.ListExecutionsResponse, error) {
	page, err := s.jobs.ListExecutions(ctx, request.GetJobId(), int(request.GetPage()), int(request.GetPageSize()))
	if err != nil {
		return nil, grpcError(err)
	}
	items := make([]*schedulerv1.Execution, 0, len(page.Items))
	for _, value := range page.Items {
		items = append(items, protoExecution(value))
	}
	return &schedulerv1.ListExecutionsResponse{Items: items, Total: page.Total, Page: int32(page.Page), PageSize: int32(page.PageSize)}, nil
}

func protoInput(name, expression, timezone, upstream, method, requestJSON string, timeout int64, enabled bool) job.Input {
	return job.Input{Name: name, CronExpression: expression, Timezone: timezone, Upstream: upstream, FullMethod: method, RequestJSON: requestJSON, TimeoutMilliseconds: timeout, Enabled: enabled}
}
func protoJob(value job.Job) *schedulerv1.Job {
	return &schedulerv1.Job{Id: value.ID, Name: value.Name, CronExpression: value.CronExpression, Timezone: value.Timezone, Upstream: value.Upstream, FullMethod: value.FullMethod, RequestJson: value.RequestJSON, TimeoutMilliseconds: value.TimeoutMilliseconds, Status: value.Status, Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}
func protoExecution(value job.Execution) *schedulerv1.Execution {
	result := &schedulerv1.Execution{Id: value.ID, JobId: value.JobID, TriggerType: value.TriggerType, Status: value.Status, ResponseJson: value.ResponseJSON, ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, StartedAt: timestamppb.New(value.StartedAt), DurationMilliseconds: value.DurationMilliseconds, Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
	if value.FinishedAt != nil {
		result.FinishedAt = timestamppb.New(*value.FinishedAt)
	}
	return result
}
func grpcError(err error) error {
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return status.Error(codes.Internal, "internal server error")
	}
	code := codes.Internal
	switch appErr.Code {
	case apperror.CodeInvalidArgument:
		code = codes.InvalidArgument
	case apperror.CodeNotFound:
		code = codes.NotFound
	case apperror.CodeUnauthorized:
		code = codes.Unauthenticated
	case apperror.CodeForbidden:
		code = codes.PermissionDenied
	case apperror.CodeConflict, apperror.CodeRequestInProgress:
		code = codes.Aborted
	case apperror.CodeDependencyUnavailable:
		code = codes.Unavailable
	case apperror.CodeRequestTimeout:
		code = codes.DeadlineExceeded
	}
	return status.Error(code, appErr.Message)
}
