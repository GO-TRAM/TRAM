package api

import (
	"context"
	"log"

	"buf.build/go/protovalidate"
	"codeberg.org/eljoth/stream/internal/ports"
	v1 "codeberg.org/eljoth/stream/pkg/gen/go/api/proto/stream/v1"
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
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &v1.PutStreamResponse{
		Stream: streamSpecToPBStream(*stream),
	}, nil
}
func (s streamServer) GetStream(ctx context.Context, req *v1.GetStreamRequest) (*v1.GetStreamResponse, error) {
	stream, err := s.core.GetStream(ctx, req.Name)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &v1.GetStreamResponse{
		Stream: streamSpecToPBStream(*stream),
	}, nil
}

func (s streamServer) DeleteStream(ctx context.Context, req *v1.DeleteStreamRequest) (*v1.DeleteStreamResponse, error) {
	if err := s.core.DeleteStream(ctx, req.Name); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &v1.DeleteStreamResponse{}, nil
}
