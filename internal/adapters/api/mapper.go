package api

import (
	"google.golang.org/protobuf/types/known/structpb"

	"codeberg.org/eljoth/tram/internal/core/domain"
	v1 "codeberg.org/eljoth/tram/pkg/gen/go/api/proto/stream/v1"
)

func pbStreamToStreamSpec(req *v1.Stream) domain.StreamSpec {
	source := pbSourceToSourceSpec(req.GetSource())
	return domain.StreamSpec{
		Name:   req.GetName(),
		Source: source,
		Stages: mmap(req.GetStages(), pbStageToStageSpec),
		Sinks:  mmap(req.GetSinks(), pbSinkToSinkSpec),
	}
}

func pbSourceToSourceSpec(req *v1.Source) domain.SourceSpec {
	return domain.SourceSpec{
		Name: req.GetName(),
		Type: req.GetType(),
		Spec: req.GetSpec().AsMap(),
	}
}

func pbStageToStageSpec(req *v1.Stage) domain.StageSpec {
	return domain.StageSpec{
		Name: req.GetName(),
		Type: req.GetType(),
		From: req.GetFrom(),
		Spec: req.GetSpec().AsMap(),
	}
}

func pbSinkToSinkSpec(req *v1.Sink) domain.SinkSpec {
	return domain.SinkSpec{
		Name: req.GetName(),
		Type: req.GetType(),
		From: req.GetFrom(),
		Spec: req.GetSpec().AsMap(),
	}
}

func streamSpecToPBStream(spec domain.StreamSpec) *v1.Stream {
	return &v1.Stream{
		Name:   spec.Name,
		Source: sourceSpecToPBSource(spec.Source),
		Stages: mmap(spec.Stages, stageSpecToPBStage),
		Sinks:  mmap(spec.Sinks, sinkSpecToPBSink),
	}
}

func sourceSpecToPBSource(spec domain.SourceSpec) *v1.Source {
	pbSpec, _ := structpb.NewValue(spec.Spec)
	return &v1.Source{
		Name: spec.Name,
		Type: spec.Type,
		Spec: pbSpec.GetStructValue(),
	}
}

func stageSpecToPBStage(spec domain.StageSpec) *v1.Stage {
	pbSpec, _ := structpb.NewValue(spec.Spec)
	return &v1.Stage{
		Name: spec.Name,
		Type: spec.Type,
		From: spec.From,
		Spec: pbSpec.GetStructValue(),
	}
}

func sinkSpecToPBSink(spec domain.SinkSpec) *v1.Sink {
	pbSpec, _ := structpb.NewValue(spec.Spec)
	return &v1.Sink{
		Name: spec.Name,
		Type: spec.Type,
		From: spec.From,
		Spec: pbSpec.GetStructValue(),
	}
}

func mmap[T, S any](in []T, f func(T) S) []S {
	out := make([]S, len(in))
	for i, v := range in {
		out[i] = f(v)
	}
	return out
}
