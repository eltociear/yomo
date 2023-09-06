package yomo

import (
	"context"
	"fmt"
	"io"

	"github.com/yomorun/yomo/core"
	"github.com/yomorun/yomo/core/frame"
	"github.com/yomorun/yomo/pkg/frame-codec/y3codec"
	"github.com/yomorun/yomo/pkg/id"
	"github.com/yomorun/yomo/pkg/trace"
)

// Source is responsible for sending data to yomo.
type Source interface {
	// Close will close the connection to YoMo-Zipper.
	Close() error
	// Connect to YoMo-Zipper.
	Connect() error
	// Write the data to directed downstream.
	Write(tag uint32, data []byte) error
	// Broadcast broadcast the data to all downstream.
	Broadcast(tag uint32, data []byte) error
	// SetErrorHandler set the error handler function when server error occurs
	SetErrorHandler(fn func(err error))
	// [Experimental] SetReceiveHandler set the observe handler function
	SetReceiveHandler(fn func(tag uint32, data []byte))
	// Pipe the data from io.Reader to YoMo-Zipper.
	Pipe(tag uint32, stream io.Reader, broadcast bool) error
}

// YoMo-Source
type yomoSource struct {
	name       string
	zipperAddr string
	client     *core.Client
	fn         func(uint32, []byte)
}

var _ Source = &yomoSource{}

// NewSource create a yomo-source
func NewSource(name, zipperAddr string, opts ...SourceOption) Source {
	clientOpts := make([]core.ClientOption, len(opts))
	for k, v := range opts {
		clientOpts[k] = core.ClientOption(v)
	}

	client := core.NewClient(name, core.ClientTypeSource, clientOpts...)

	return &yomoSource{
		name:       name,
		zipperAddr: zipperAddr,
		client:     client,
	}
}

// Close will close the connection to YoMo-Zipper.
func (s *yomoSource) Close() error {
	if err := s.client.Close(); err != nil {
		s.client.Logger().Error("failed to close the source", "err", err)
		return err
	}
	s.client.Logger().Debug("the source is closed")
	return nil
}

// Connect to YoMo-Zipper.
func (s *yomoSource) Connect() error {
	// set backflowframe handler
	s.client.SetBackflowFrameObserver(func(frm *frame.BackflowFrame) {
		if s.fn != nil {
			s.fn(frm.Tag, frm.Carriage)
		}
	})

	err := s.client.Connect(context.Background(), s.zipperAddr)
	return err
}

// Write writes data with specified tag.
func (s *yomoSource) Write(tag uint32, data []byte) error {
	return s.write(tag, data, false)
}

// SetErrorHandler set the error handler function when server error occurs
func (s *yomoSource) SetErrorHandler(fn func(err error)) {
	s.client.SetErrorHandler(fn)
}

// [Experimental] SetReceiveHandler set the observe handler function
func (s *yomoSource) SetReceiveHandler(fn func(uint32, []byte)) {
	s.fn = fn
	s.client.Logger().Info("receive hander set for the source")
}

// Broadcast write the data to all downstreams.
func (s *yomoSource) Broadcast(tag uint32, data []byte) error {
	return s.write(tag, data, true)
}

func (s *yomoSource) write(tag uint32, data []byte, broadcast bool) error {
	var tid, sid string
	// trace
	tp := s.client.TracerProvider()
	traced := false
	if tp != nil {
		span, err := trace.NewSpan(tp, core.StreamTypeSource.String(), s.name, "", "")
		if err != nil {
			s.client.Logger().Error("source trace error", "err", err)
		} else {
			defer span.End()
			tid = span.SpanContext().TraceID().String()
			sid = span.SpanContext().SpanID().String()
			traced = true
		}
	}
	if tid == "" {
		s.client.Logger().Debug("source create new tid")
		tid = id.TID()
	}
	if sid == "" {
		s.client.Logger().Debug("source create new sid")
		sid = id.SID()
	}
	s.client.Logger().Debug("source metadata", "tid", tid, "sid", sid, "broadcast", broadcast, "traced", traced)
	// metadata
	md, err := core.NewDefaultMetadata(s.client.ClientID(), broadcast, tid, sid, traced).Encode()
	if err != nil {
		return err
	}
	f := &frame.DataFrame{
		Tag:      tag,
		Metadata: md,
		Payload:  data,
	}
	s.client.Logger().Debug("source write", "tag", tag, "data", data, "broadcast", broadcast)
	return s.client.WriteFrame(f)
}

func (s *yomoSource) Pipe(tag uint32, stream io.Reader, broadcast bool) error {
	return s.pipe(tag, stream, broadcast)
}

// Pipe the data from io.Reader to YoMo-Zipper.
func (s *yomoSource) pipe(tag uint32, stream io.Reader, broadcast bool) error {
	// request stream
	dataStream, err := s.client.RequestStream(context.Background(), s.zipperAddr, stream)
	if err != nil {
		return err
	}
	// TODO: trace

	// metadata
	tid := id.TID()
	sid := id.SID()
	traced := true
	md, err := core.NewDefaultMetadata(s.client.ClientID(), broadcast, tid, sid, traced).Encode()
	if err != nil {
		return err
	}
	// write frame
	// TODO: 从服务端获取
	buf := make([]byte, 1024)
	streamFrame := &frame.StreamFrame{
		ID:        dataStream.ID(),
		StreamID:  dataStream.StreamID(),
		ChunkSize: uint(len(buf)),
	}
	// TODO: 硬编码,需要修改
	data, err := y3codec.Codec().Encode(streamFrame)
	if err != nil {
		return err
	}
	f := &frame.DataFrame{
		Tag:      tag,
		Metadata: md,
		Payload:  data,
		Streamed: true,
	}
	err = s.client.WriteFrame(f)
	if err != nil {
		s.client.Logger().Error("source write frame error", "err", err)
		return err
	}
	// s.client.Logger().Debug("source pipe", "tag", tag, "data", data, "broadcast", broadcast)
	fmt.Printf("source pipe: tag=%v, data=%+v, broadcast=%v, streamed=%v\n", tag, streamFrame, broadcast, f.Streamed)
	// sync stream
	_, err = io.CopyBuffer(dataStream, stream, buf)
	if err != nil {
		if err == io.EOF {
			s.client.Logger().Info("source sync stream done", "stream_id", dataStream.StreamID)
			return nil
		}
		s.client.Logger().Error("source sync stream error", "err", err)
		return err
	}
	return nil
}
