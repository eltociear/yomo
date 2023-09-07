package core

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yomorun/yomo/core/frame"
	"github.com/yomorun/yomo/core/metadata"
	"github.com/yomorun/yomo/core/router"
	"github.com/yomorun/yomo/core/yerr"
	"golang.org/x/exp/slog"
)

// StreamGroup is a group of streams includes ControlStream amd DataStream.
// A single Connection can have multiple DataStreams, but only one ControlStream.
// The ControlStream receives HandshakeFrames to create DataStreams, while
// the DataStreams receive and broadcast DataFrames to other DataStreams.
// the ControlStream is always the first stream established between server and client.
type StreamGroup struct {
	ctx           context.Context
	baseMetadata  metadata.M
	controlStream *ServerControlStream
	connector     *Connector
	router        router.Router
	logger        *slog.Logger
	group         sync.WaitGroup
}

// NewStreamGroup returns the StreamGroup.
func NewStreamGroup(
	ctx context.Context,
	baseMetadata metadata.M,
	controlStream *ServerControlStream,
	connector *Connector,
	router router.Router,
	logger *slog.Logger,
) *StreamGroup {
	group := &StreamGroup{
		ctx:           ctx,
		baseMetadata:  baseMetadata,
		controlStream: controlStream,
		connector:     connector,
		router:        router,
		logger:        logger,
	}
	logger.Info("connection connected")

	return group
}

func (g *StreamGroup) handleRoute(hf *frame.HandshakeFrame, md metadata.M) (router.Route, error) {
	if hf.ClientType != byte(ClientTypeStreamFunction) {
		return nil, nil
	}
	// route for sfn.
	route := g.router.Route(md)
	if route == nil {
		return nil, errors.New("yomo: can't find route in handshake metadata")
	}
	err := route.Add(hf.ID, hf.Name, hf.ObserveDataTags)
	if err == nil {
		return route, nil
	}
	// If there is a stream with the same name as the new stream, replace the old stream with the new one.
	if e := new(yerr.DuplicateNameError); errors.As(err, e) {
		existsStreamID := e.StreamID()
		stream, ok, err := g.connector.Get(existsStreamID)
		if err != nil {
			return nil, err
		}
		if ok {
			stream.Close()
			g.connector.Delete(existsStreamID)
			g.logger.Debug("connector remove stream", "id", stream.ID(), "client_type", stream.ClientType().String(), "name", stream.Name())
		}
	}
	return route, nil
}

type handshakeResult struct {
	route router.Route
}

// makeHandshakeFunc creates a function that will handle a HandshakeFrame.
// It takes route parameter, which will be assigned after the returned function is executed.
func (g *StreamGroup) makeHandshakeFunc(result *handshakeResult) func(hf *frame.HandshakeFrame) (metadata.M, error) {
	return func(hf *frame.HandshakeFrame) (metadata.M, error) {
		_, ok, err := g.connector.Get(hf.ID)
		if err != nil {
			return metadata.M{}, err
		}
		if ok {
			return metadata.M{}, fmt.Errorf("yomo: stream id[%s] is not allowed to be a duplicate", hf.ID)
		}

		md, err := metadata.Decode(hf.Metadata)
		if err != nil {
			return metadata.M{}, err
		}

		// merge base metadata
		g.baseMetadata.Range(func(k, v string) bool {
			md.Set(k, v)
			return true
		})

		route, err := g.handleRoute(hf, md)
		if err != nil {
			return metadata.M{}, err
		}
		result.route = route
		// BUG: should be return md, nil
		return metadata.M{}, err
	}
}

// Run run contextFunc with connector.
// Run continuous Accepts DataStream and create a Context to run with contextFunc.
// TODO: run in aop model, like before -> handle -> after.
func (g *StreamGroup) Run(contextFunc func(c *Context)) error {
	for {
		var routeResult handshakeResult

		handshakeFunc := g.makeHandshakeFunc(&routeResult)

		stream, err := g.controlStream.OpenStream(g.ctx, handshakeFunc)
		if err != nil {
			g.logger.Error("control stream open stream error", "err", err)
			return err
		}
		g.group.Add(1)
		g.connector.Store(stream.ID(), stream)
		g.logger.Debug("connector add stream", "id", stream.ID(), "stream_id", stream.StreamID(), "client_type", stream.ClientType().String(), "name", stream.Name())

		go g.handleContextFunc(routeResult.route, stream, contextFunc)
	}
}

func (g *StreamGroup) handleContextFunc(route router.Route, stream DataStream, contextFunc func(c *Context)) {
	defer func() {
		// source route is always nil.
		if route != nil {
			route.Remove(stream.ID())
		}
		g.connector.Delete(stream.ID())
		g.logger.Debug("connector remove stream", "id", stream.ID(), "stream_id", stream.StreamID(), "client_type", stream.ClientType().String(), "name", stream.Name())
		g.group.Done()
	}()

	c := newContext(stream, route, g.logger)
	defer c.Release()

	contextFunc(c)
}

// Wait waits all dataStream down.
func (g *StreamGroup) Wait() { g.group.Wait() }

// StreamKey returns the key of stream.
// func StreamKey(clientID string, streamID int64) string {
// 	return fmt.Sprintf("%s|%d", clientID, streamID)
// }
