package otlp

type Spec struct {
	HttpPort int `json:"http_port" validate:"required"`
	GRPCPort int `json:"grpc_port" validate:"required"`
}
