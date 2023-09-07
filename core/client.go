// Package core provides the core functions of YoMo.
package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/yomorun/yomo/core/frame"
	"github.com/yomorun/yomo/pkg/frame-codec/y3codec"
	"github.com/yomorun/yomo/pkg/id"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"
)

// Client is the abstraction of a YoMo-Client. a YoMo-Client can be
// Source, Upstream Zipper or StreamFunction.
type Client struct {
	name           string                     // name of the client
	clientID       string                     // id of the client
	clientType     ClientType                 // type of the client
	processor      func(*frame.DataFrame)     // function to invoke when data arrived
	receiver       func(*frame.BackflowFrame) // function to invoke when data is processed
	errorfn        func(error)                // function to invoke when error occured
	opts           *clientOptions
	logger         *slog.Logger
	tracerProvider oteltrace.TracerProvider

	// ctx and ctxCancel manage the lifecycle of client.
	ctx       context.Context
	ctxCancel context.CancelFunc

	writeFrameChan chan frame.Frame
	// control stream
	controlStream *ClientControlStream
}

// NewClient creates a new YoMo-Client.
func NewClient(appName string, clientType ClientType, opts ...ClientOption) *Client {
	option := defaultClientOption()

	for _, o := range opts {
		o(option)
	}
	clientID := id.New()

	logger := option.logger.With("component", clientType.String(), "client_id", clientID, "client_name", appName)

	if option.credential != nil {
		logger.Info("use credential", "credential_name", option.credential.Name())
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	return &Client{
		name:           appName,
		clientID:       clientID,
		clientType:     clientType,
		opts:           option,
		logger:         logger,
		tracerProvider: option.tracerProvider,
		errorfn:        func(err error) { logger.Error("client err", "err", err) },
		writeFrameChan: make(chan frame.Frame),
		ctx:            ctx,
		ctxCancel:      ctxCancel,
	}
}

// Connect connect client to server.
func (c *Client) Connect(ctx context.Context, addr string) error {
	if c.clientType == ClientTypeStreamFunction && len(c.opts.observeDataTags) == 0 {
		return errors.New("yomo: streamFunction cannot observe data because the required tag has not been set")
	}

	c.logger = c.logger.With("zipper_addr", addr)

connect:
	// TODO: Step 1
	// controlStream, dataStream, err := c.openStream(ctx, addr)
	controlStream, err := c.openControlStream(ctx, addr)
	if err != nil {
		if c.opts.connectUntilSucceed && !errors.As(err, new(ErrAuthenticateFailed)) {
			c.logger.Error("failed to connect to zipper, trying to reconnect", "err", err)
			time.Sleep(time.Second)
			goto connect
		}
		c.logger.Error("can not connect to zipper", "error", err)
		return err
	}
	c.logger = c.logger.With("local_addr", controlStream.Conn().LocalAddr())
	c.logger.Info("connected to zipper")

	// set control stream
	c.setControlStream(controlStream)

	// TODO: Step 2
	// go c.runBackground(ctx, addr, controlStream, dataStream)
	go c.runBackground(ctx, addr, controlStream)

	return nil
}

func (c *Client) runBackground(ctx context.Context, addr string, controlStream *ClientControlStream) {
	reconnection := make(chan struct{})
	// go c.processStream(controlStream, dataStream, reconnection)
	// 使用控制流处理数据流
	controlStream = c.ControlStream()
	// TODO: Step 3
	// 读取控制流中所有数据流, 并处理数据流
	go c.processStream(controlStream, reconnection)

	for {
		select {
		case <-c.ctx.Done():
			c.cleanStream(controlStream, nil)
			return
		case <-ctx.Done():
			c.cleanStream(controlStream, ctx.Err())
			return
		case <-reconnection:
		reconnect:
			var err error
			// TODO: Step 4
			// controlStream, dataStream, err = c.openStream(ctx, addr)
			controlStream, err = c.openControlStream(ctx, addr)
			if err != nil {
				if errors.As(err, new(ErrAuthenticateFailed)) {
					c.cleanStream(controlStream, err)
					return
				}
				c.logger.Error("reconnect error", "err", err)
				time.Sleep(time.Second)
				goto reconnect
			}
			// go c.processStream(controlStream, dataStream, reconnection)
			c.setControlStream(controlStream)
			go c.processStream(controlStream, reconnection)
		}
	}
}

// func (c *Client) runBackground(ctx context.Context, addr string, controlStream *ClientControlStream, dataStream DataStream) {
// 	reconnection := make(chan struct{})
//
// 	go c.processStream(controlStream, dataStream, reconnection)
//
// 	for {
// 		select {
// 		case <-c.ctx.Done():
// 			c.cleanStream(controlStream, nil)
// 			return
// 		case <-ctx.Done():
// 			c.cleanStream(controlStream, ctx.Err())
// 			return
// 		case <-reconnection:
// 		reconnect:
// 			var err error
// 			controlStream, dataStream, err = c.openStream(ctx, addr)
// 			if err != nil {
// 				if errors.As(err, new(ErrAuthenticateFailed)) {
// 					c.cleanStream(controlStream, err)
// 					return
// 				}
// 				c.logger.Error("reconnect error", "err", err)
// 				time.Sleep(time.Second)
// 				goto reconnect
// 			}
// 			go c.processStream(controlStream, dataStream, reconnection)
// 		}
// 	}
// }

// WriteFrame write frame to client.
func (c *Client) WriteFrame(f frame.Frame) error {
	if c.opts.nonBlockWrite {
		return c.nonBlockWriteFrame(f)
	}
	return c.blockWriteFrame(f)
}

// blockWriteFrame writes frames in block mode, guaranteeing that frames are not lost.
func (c *Client) blockWriteFrame(f frame.Frame) error {
	c.writeFrameChan <- f
	return nil
}

// nonBlockWriteFrame writes frames in non-blocking mode, without guaranteeing that frames will not be lost.
func (c *Client) nonBlockWriteFrame(f frame.Frame) error {
	select {
	case c.writeFrameChan <- f:
		return nil
	default:
		err := errors.New("yomo: client has lost connection")
		c.logger.Debug("failed to write frame", "frame_type", f.Type().String(), "error", err)
		return err
	}
}

func (c *Client) cleanStream(controlStream *ClientControlStream, err error) {
	errString := ""
	if err != nil {
		errString = err.Error()
		c.logger.Error("client exit", "err", err)
	}

	// controlStream is nil represents that client is not connected.
	if controlStream == nil {
		return
	}

	controlStream.CloseWithError(errString)
}

// Close close the client.
func (c *Client) Close() error {
	// break runBackgroud() for-loop.
	c.ctxCancel()

	return nil
}

func (c *Client) openControlStream(ctx context.Context, addr string) (*ClientControlStream, error) {
	controlStream, err := OpenClientControlStream(
		ctx, addr,
		c.opts.tlsConfig, c.opts.quicConfig,
		y3codec.Codec(), y3codec.PacketReadWriter(),
		c.logger,
	)
	if err != nil {
		return controlStream, err
	}

	if err := controlStream.Authenticate(c.opts.credential); err != nil {
		return controlStream, err
	}

	return controlStream, nil
}

// func (c *Client) openStream(ctx context.Context, addr string) (*ClientControlStream, DataStream, error) {
// 	controlStream, err := c.openControlStream(ctx, addr)
// 	if err != nil {
// 		return controlStream, nil, err
// 	}
// 	dataStream, err := c.openDataStream(ctx, controlStream)
// 	if err != nil {
// 		return controlStream, dataStream, err
// 	}
//
// 	return controlStream, dataStream, nil
// }

func (c *Client) openDataStream(ctx context.Context, controlStream *ClientControlStream) (DataStream, error) {
	handshakeFrame := &frame.HandshakeFrame{
		Name:            c.name,
		ID:              id.New(),
		ClientID:        c.clientID,
		ClientType:      byte(c.clientType),
		ObserveDataTags: c.opts.observeDataTags,
	}

	err := controlStream.RequestStream(handshakeFrame)
	if err != nil {
		c.logger.Error("request stream error", "err", err)
		return nil, err
	}

	return controlStream.AcceptStream(ctx)
}

// func (c *Client) processStream(controlStream *ClientControlStream, dataStream DataStream, reconnection chan<- struct{}) {
func (c *Client) processStream(controlStream *ClientControlStream, reconnection chan<- struct{}) {
	// 从控制流中读取数据流
	dataStream, err := c.openDataStream(c.ctx, controlStream)
	if err != nil {
		c.handleFrameError(err, reconnection)
		return
	}
	defer dataStream.Close()

	readFrameChan := c.readFrame(dataStream)

	for {
		select {
		case result := <-readFrameChan:
			if err := result.err; err != nil {
				c.handleFrameError(err, reconnection)
				return
			}
			func() {
				defer func() {
					if e := recover(); e != nil {
						const size = 64 << 10
						buf := make([]byte, size)
						buf = buf[:runtime.Stack(buf, false)]

						perr := fmt.Errorf("%v", e)
						c.logger.Error("stream panic", "err", perr)
						c.errorfn(fmt.Errorf("yomo: stream panic: %v\n%s", perr, buf))
					}
				}()
				c.handleFrame(result.frame)
			}()
		case f := <-c.writeFrameChan:
			if err := dataStream.WriteFrame(f); err != nil {
				c.handleFrameError(err, reconnection)
				return
			}
		}
	}
}

// handleFrameError handles errors that occur during frame reading and writing by performing the following actions:
// Sending the error to the error function (errorfn).
// Closing the client if the data stream has been closed.
// Always attempting to reconnect if an error is encountered.
func (c *Client) handleFrameError(err error, reconnection chan<- struct{}) {
	if err == nil {
		return
	}

	c.errorfn(err)

	// exit client program if stream has be closed.
	if err == io.EOF {
		c.ctxCancel()
		return
	}

	// always attempting to reconnect if an error is encountered,
	// the error is mostly network error.
	select {
	case reconnection <- struct{}{}:
	default:
	}
}

// Wait waits client returning.
func (c *Client) Wait() {
	<-c.ctx.Done()
}

type readResult struct {
	frame frame.Frame
	err   error
}

func (c *Client) readFrame(dataStream DataStream) chan readResult {
	readChan := make(chan readResult)
	go func() {
		for {
			f, err := dataStream.ReadFrame()
			readChan <- readResult{f, err}
			if err != nil {
				return
			}
		}
	}()

	return readChan
}

func (c *Client) handleFrame(f frame.Frame) {
	switch ff := f.(type) {
	case *frame.DataFrame:
		if c.processor == nil {
			c.logger.Warn("the processor has not been set")
		} else {
			c.processor(ff)
		}
	case *frame.BackflowFrame:
		if c.receiver == nil {
			c.logger.Warn("the receiver has not been set")
		} else {
			c.receiver(ff)
		}
	default:
		c.logger.Warn("data stream received unexpected frame", "frame_type", f.Type().String())
	}
}

// SetDataFrameObserver sets the data frame handler.
func (c *Client) SetDataFrameObserver(fn func(*frame.DataFrame)) {
	c.processor = fn
}

// SetBackflowFrameObserver sets the backflow frame handler.
func (c *Client) SetBackflowFrameObserver(fn func(*frame.BackflowFrame)) {
	c.receiver = fn
}

// SetObserveDataTags set the data tag list that will be observed.
func (c *Client) SetObserveDataTags(tag ...frame.Tag) {
	c.opts.observeDataTags = tag
}

// Logger get client's logger instance, you can customize this using `yomo.WithLogger`
func (c *Client) Logger() *slog.Logger {
	return c.logger
}

// SetErrorHandler set error handler
func (c *Client) SetErrorHandler(fn func(err error)) {
	c.errorfn = fn
	c.logger.Debug("the error handler has been set")
}

// ClientID returns the ID of client.
func (c *Client) ClientID() string { return c.clientID }

// Name returns the name of client.
func (c *Client) Name() string { return c.name }

// FrameWriterConnection represents a frame writer that can connect to an addr.
type FrameWriterConnection interface {
	frame.Writer
	Name() string
	Close() error
	Connect(context.Context, string) error
}

// TracerProvider returns the tracer provider of client.
func (c *Client) TracerProvider() oteltrace.TracerProvider {
	if c.tracerProvider == nil {
		return nil
	}
	if reflect.ValueOf(c.tracerProvider).IsNil() {
		return nil
	}
	return c.tracerProvider
}

// TODO: 考虑如何实现这个接口兼容 stream
// Write writes data to client.
func (c *Client) Write(p []byte) (int, error) {
	return 0, nil
}

// RequestStream request a stream from server.
// func (c *Client) RequestStream(ctx context.Context, addr string, reader io.Reader) (*frame.StreamFrame, error) {
func (c *Client) RequestStream(ctx context.Context, addr string, reader io.Reader) (DataStream, error) {
	// controlStream, err := c.openControlStream(ctx, addr)
	// if err != nil {
	// 	c.errorfn(err)
	// 	return nil, err
	// }
	c.logger.Debug("client request stream")
	dataStream, err := c.openDataStream(c.ctx, c.ControlStream())
	if err != nil {
		if err == io.EOF {
			c.logger.Info("client request stream EOF")
			dataStream.Close()
			return nil, err
		}
		c.logger.Error("client request stream error", "err", err)
		c.errorfn(err)
		return nil, err
	}
	c.logger.Info("client request stream success", "id", dataStream.ID(), "stream_id", dataStream.StreamID())
	return dataStream, nil
}

// ControlStream returns the control stream of client.
func (c *Client) ControlStream() *ClientControlStream {
	return (*ClientControlStream)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.controlStream))))
}

// setControlStream sets the control stream of client.
func (c *Client) setControlStream(controlStream *ClientControlStream) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&c.controlStream)), unsafe.Pointer(controlStream))
}
