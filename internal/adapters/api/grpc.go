package api

import (
	"context"
	"log"

	"codeberg.org/eljoth/stream/internal/ports"
	v1 "codeberg.org/eljoth/stream/pkg/gen/go/api/proto/stream/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type streamServer struct {
	v1.UnimplementedStreamServiceServer
	core ports.Core
}

func (s streamServer) PutStream(context.Context, *v1.PutStreamRequest) (*v1.PutStreamResponse, error) {
	log.Println("CALLED")
	return nil, status.Error(codes.Unimplemented, "method PutStream not implemented")
}
func (s streamServer) GetStream(context.Context, *v1.GetStreamRequest) (*v1.GetStreamResponse, error) {
	log.Println("CALLED")
	return nil, status.Error(codes.Unimplemented, "method GetStream not implemented")
}
func (s streamServer) DeleteStream(context.Context, *v1.DeleteStreamRequest) (*v1.DeleteStreamResponse, error) {
	log.Println("CALLED")
	return nil, status.Error(codes.Unimplemented, "method DeleteStream not implemented")
}
