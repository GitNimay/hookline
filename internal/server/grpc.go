package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// NewGRPCServer builds a gRPC server with the standard interceptor chain:
// recovery, tracing (stats handler), metrics, logging, error mapping, then the given extras (auth).
func NewGRPCServer(log *slog.Logger, extra ...grpc.UnaryServerInterceptor) *grpc.Server {
	chain := append([]grpc.UnaryServerInterceptor{recovery(log), metrics, logging(log), mapErrors(log)}, extra...)
	return grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(chain...),
		grpc.MaxRecvMsgSize(1<<20),
	)
}

func recovery(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "panic", "method", info.FullMethod, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return h(ctx, req)
	}
}

func metrics(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := h(ctx, req)
	svc, method := splitMethod(info.FullMethod)
	code := status.Code(err).String()
	telemetry.GRPCHandled.WithLabelValues(svc, method, code).Observe(time.Since(start).Seconds())
	if svc == "hookline.v1.IngestService" {
		telemetry.IngestDuration.WithLabelValues(method, code).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func logging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		code := status.Code(err)
		lvl := slog.LevelDebug
		if code == codes.Internal || code == codes.Unknown || code == codes.Unavailable {
			lvl = slog.LevelError
		}
		log.Log(ctx, lvl, "rpc", "method", info.FullMethod, "code", code.String(), "duration_ms", time.Since(start).Milliseconds(), "err", errString(err))
		return resp, err
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func mapErrors(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if err == nil {
			return resp, nil
		}
		st := ToStatus(err)
		if st.Code() == codes.Internal {
			log.ErrorContext(ctx, "internal error", "method", info.FullMethod, "err", err)
		}
		return resp, st.Err()
	}
}

// ToStatus maps domain errors to gRPC status. This is the only place that mapping lives.
func ToStatus(err error) *status.Status {
	if st, ok := status.FromError(err); ok {
		return st
	}
	var fe *domain.FieldError
	switch {
	case errors.As(err, &fe):
		st := status.New(codes.InvalidArgument, fe.Error())
		if d, derr := st.WithDetails(&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: fe.Field, Description: fe.Reason}}}); derr == nil {
			return d
		}
		return st
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.New(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.New(codes.NotFound, "not found")
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.New(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrUnauthenticated):
		return status.New(codes.Unauthenticated, "invalid or missing API key")
	case errors.Is(err, domain.ErrPermission):
		return status.New(codes.PermissionDenied, "permission denied")
	case errors.Is(err, domain.ErrUnavailable):
		return status.New(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.New(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.New(codes.Canceled, "canceled")
	}
	return status.New(codes.Internal, "internal error")
}

func splitMethod(full string) (string, string) {
	full = strings.TrimPrefix(full, "/")
	svc, m, _ := strings.Cut(full, "/")
	return svc, m
}

// SetHTTPCode makes the REST gateway answer with code instead of 200.
func SetHTTPCode(ctx context.Context, code int) {
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-http-code", strconv.Itoa(code)))
}

// SetRetryAfter adds a Retry-After header (seconds) to the REST response.
func SetRetryAfter(ctx context.Context, d time.Duration) {
	_ = grpc.SetHeader(ctx, metadata.Pairs("retry-after", strconv.Itoa(int(d.Seconds()+0.999))))
}

// DialLocal opens a client connection to a gRPC server, with tracing and metrics.
func DialLocal(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(clientMetrics),
	)
}

func clientMetrics(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	telemetry.GRPCClientHandled.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return err
}

// MaxBodyBytes is the REST request body limit.
const MaxBodyBytes = 512 << 10

// NewGateway returns the REST mux plus an http.Handler that wraps it with
// tracing and the body size limit. Register services on the mux.
func NewGateway() (*runtime.ServeMux, http.Handler) {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true},
			UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: false},
		}),
		runtime.WithOutgoingHeaderMatcher(func(k string) (string, bool) {
			if k == "retry-after" {
				return "Retry-After", true
			}
			return "", false
		}),
		runtime.WithForwardResponseOption(func(ctx context.Context, w http.ResponseWriter, _ proto.Message) error {
			md, ok := runtime.ServerMetadataFromContext(ctx)
			if !ok {
				return nil
			}
			if v := md.HeaderMD.Get("x-http-code"); len(v) > 0 {
				if code, err := strconv.Atoi(v[0]); err == nil {
					w.WriteHeader(code)
				}
			}
			return nil
		}),
	)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		mux.ServeHTTP(w, r)
	})
	return mux, otelhttp.NewHandler(h, "gateway")
}
