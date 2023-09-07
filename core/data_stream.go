package core

import (
	"context"
	"io"

	"github.com/yomorun/yomo/core/frame"
	"github.com/yomorun/yomo/core/metadata"
)

// StreamInfo holds the information of DataStream.
type StreamInfo interface {
	// Name returns the name of the stream, which is set by clients.
	Name() string
	// ID represents the dataStream ID, the ID is an unique string.
	ID() string
	// StreamID represents the stream ID, the ID is an unique int64.
	StreamID() int64
	// ClientType represents client type (Source | SFN | UpstreamZipper).
	ClientType() ClientType
	// Metadata returns the extra info of the application.
	// The metadata is a merged set of data from both the handshake and authentication processes.
	Metadata() metadata.M
	// ObserveDataTags observed data tags.
	// TODO: There maybe a sorted list, we can find tag quickly.
	ObserveDataTags() []frame.Tag
}

// DataStream wraps the specific io stream (typically quic.Stream) to transfer frames.
// DataStream be used to read and write frames, and be managed by Connector.
type DataStream interface {
	Context() context.Context
	StreamInfo
	frame.ReadWriteCloser
}

type dataStream struct {
	name       string
	id         string
	clientType ClientType
	metadata   metadata.M
	observed   []frame.Tag

	stream *FrameStream
}

// newDataStream constructures dataStream.
func newDataStream(
	name string,
	id string,
	clientType ClientType,
	metadata metadata.M,
	observed []frame.Tag,
	stream *FrameStream,
) DataStream {
	return &dataStream{
		name:       name,
		id:         id,
		clientType: clientType,
		metadata:   metadata,
		observed:   observed,
		stream:     stream,
	}
}

// DataStream implements.
func (s *dataStream) Context() context.Context        { return s.stream.Context() }
func (s *dataStream) ID() string                      { return s.id }
func (s *dataStream) Name() string                    { return s.name }
func (s *dataStream) Metadata() metadata.M            { return s.metadata }
func (s *dataStream) ClientType() ClientType          { return s.clientType }
func (s *dataStream) ObserveDataTags() []frame.Tag    { return s.observed }
func (s *dataStream) WriteFrame(f frame.Frame) error  { return s.stream.WriteFrame(f) }
func (s *dataStream) ReadFrame() (frame.Frame, error) { return s.stream.ReadFrame() }
func (s *dataStream) Close() error                    { return s.stream.Close() }

func (s *dataStream) StreamID() int64             { return s.stream.StreamID() }
func (s *dataStream) Write(p []byte) (int, error) { return s.stream.Write(p) }

// ContextReadWriteCloser represents a stream which its lifecycle managed by context.
// The context should be closed when the stream is closed.
type ContextReadWriteCloser interface {
	// Context returns the context which manage the lifecycle of stream.
	Context() context.Context
	// The stream.
	io.ReadWriteCloser
	// Stream
	Stream
}

// Stream represents a stream.
type Stream interface {
	StreamID() int64
}
