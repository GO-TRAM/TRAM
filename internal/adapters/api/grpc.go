package api

import (
	"context"
	"errors"
	"log"

	"buf.build/go/protovalidate"
	"codeberg.org/eljoth/tram/internal/core/domain"
	"codeberg.org/eljoth/tram/internal/ports"
	v1 "codeberg.org/eljoth/tram/pkg/gen/go/api/proto/stream/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type streamServer struct {
	v1.UnimplementedStreamServiceServer
	core ports.Core
}

func (s streamServer) PutStream(ctx context.Context, req *v1.PutStreamRequest) (*v1.PutStreamResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	streamSpec := pbStreamToStreamSpec(req.Stream)

	stream, err := s.core.UpsertStream(ctx, streamSpec)
	if err != nil {
		return nil, toStatus(err)
	}

	return &v1.PutStreamResponse{
		Stream: streamSpecToPBStream(*stream),
	}, nil
}
func (s streamServer) GetStream(ctx context.Context, req *v1.GetStreamRequest) (*v1.GetStreamResponse, error) {
	stream, err := s.core.GetStream(ctx, req.Name)
	if err != nil {
		return nil, toStatus(err)
	}
	return &v1.GetStreamResponse{
		Stream: streamSpecToPBStream(*stream),
	}, nil
}

func (s streamServer) DeleteStream(ctx context.Context, req *v1.DeleteStreamRequest) (*v1.DeleteStreamResponse, error) {
	if err := s.core.DeleteStream(ctx, req.Name); err != nil {
		return nil, toStatus(err)
	}
	return &v1.DeleteStreamResponse{}, nil
}

// toStatus maps core errors to gRPC status codes, which the gateway turns
// into the matching HTTP status.
func toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrStreamNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrStreamBuildFailed):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrCoreClosed):
		return status.Error(codes.Unavailable, err.Error())
	default:
		log.Println(err)
		return status.Error(codes.Internal, err.Error())
	}
}
