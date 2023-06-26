package core

import (
	"context"
	"io"
	"sync"

	"golang.org/x/exp/slog"
)

// Peer represents a peer in a network that can open a writer and observe tagged streams,
// and handle them in an observer.
type Peer struct {
	once sync.Once
	// the tag of the writer in the ObserveHandler
	observeHandlerWriterTag string

	conn          UniStreamPeerConnection
	tagWriterFunc func(string, io.Writer) error
}

// NewPeer returns a new peer.
func NewPeer(conn UniStreamPeerConnection, tagWriterFunc func(string, io.Writer) error) *Peer {
	peer := &Peer{
		conn:          conn,
		tagWriterFunc: tagWriterFunc,
	}

	return peer
}

// SetObserveHandlerWriterTag sets a tag for other peer can observe the writer stream in Observers handler.
// If this function is not called, writing to the writer in the ObserverHandler will not do anything.
// Note That multiple calling this function will have no effect.
func (p *Peer) SetObserveHandlerWriterTag(tag string) {
	p.once.Do(func() {
		p.observeHandlerWriterTag = tag
	})
}

// Open opens a writer with the given tag, which other peers can observe.
// The returned writer can be used to write to the stream associated with the given tag.
func (p *Peer) Open(tag string) (io.WriteCloser, error) {
	w, err := p.conn.OpenUniStream()
	if err != nil {
		return nil, err
	}

	return w, p.tagWriterFunc(tag, w)
}

// Observe observes tagged streams and handles them in an observer.
// The observer is responsible for handling the tagged streams and writing to a new peer stream.
func (p *Peer) Observe(tag string, observer Observer) error {
	// peer request to observe stream in the specified tag.
	err := p.conn.RequestObserve(tag)
	if err != nil {
		return err
	}
	// then waiting and handling the stream reponsed by server.
	return p.observing(observer)
}

func (p *Peer) observing(observer Observer) error {
	for {
		r, err := p.conn.AcceptUniStream(context.Background())
		if err != nil {
			return err
		}
		var w io.Writer
		// discard writer if there is no tag.
		if p.observeHandlerWriterTag != "" {
			w, err = p.Open(p.observeHandlerWriterTag)
			if err != nil {
				return err
			}
			err = p.tagWriterFunc(p.observeHandlerWriterTag, w)
			if err != nil {
				return err
			}
		} else {
			w = io.Discard
		}
		observer.Handle(r, w)
	}
}

// Broker accepts streams from Peer and docks them to another Peer.
type Broker struct {
	ctx             context.Context
	ctxCancel       context.CancelFunc
	readerChan      chan taggedReader
	readEOFChan     chan string // if read EOF, send to this chan
	observerChan    chan taggedConnection
	logger          *slog.Logger
	drainReaderFunc func(io.Reader) (string, error)
}

// NewBroker creates a new broker.
// The broker accepts streams from Peer and docks them to another Peer.
func NewBroker(ctx context.Context, drainReaderFunc func(io.Reader) (string, error), logger *slog.Logger) *Broker {
	ctx, ctxCancel := context.WithCancel(ctx)

	broker := &Broker{
		ctx:             ctx,
		ctxCancel:       ctxCancel,
		readerChan:      make(chan taggedReader),
		readEOFChan:     make(chan string),
		observerChan:    make(chan taggedConnection),
		logger:          logger,
		drainReaderFunc: drainReaderFunc,
	}

	go broker.run()

	return broker
}

// AcceptingStream continusly accepts uniStreams from conn and retrives the tag from the reader accepted.
// It will block until the accepter receive an error.
func (b *Broker) AcceptingStream(conn UniStreamConnection) {
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}
		r, err := conn.AcceptUniStream(b.ctx)
		if err != nil {
			b.logger.Debug("failed to accept a uniStream", "error", err)
			break
		}
		tag, err := b.drainReaderFunc(r)
		if err != nil {
			b.logger.Debug("ack peer stream failed", "error", err)
			continue
		}
		b.readerChan <- taggedReader{r: r, tag: tag}
	}
}

// Observe makes the conn observe the given tag.
// If an conn observes a tag, it will be notified to open a new stream to dock with
// the tagged stream when it arrives.
func (b *Broker) Observe(tag string, conn UniStreamConnection) {
	item := taggedConnection{
		tag:  tag,
		conn: conn,
	}
	b.logger.Debug("accept an observer", "tag", tag, "conn_id", conn.ID())
	b.observerChan <- item
}

// Close closes the broker.
func (b *Broker) Close() error {
	b.ctxCancel()
	return nil
}

func (b *Broker) run() {
	var (
		// observers is a collection of connections.
		// The keys in observers are tags that are used to identify the observers.
		// The values in observers are maps where the keys are observer IDs and the values are the observers themselves.
		// The value maps ensure that each ID has only one corresponding observer.
		observers = make(map[string]map[string]UniStreamConnection)

		// readers stores readers. The key is the tag and the value is the reader.
		// Using a map means that each tag only has one corresponding reader and
		// new stream cannot cover the old stream in same tag.
		readers = make(map[string]io.ReadCloser)
	)
	for {
		select {
		case <-b.ctx.Done():
			b.logger.Debug("broker is closed")
			return
		case o := <-b.observerChan:
			// if the writer opener is already registered, observe the writer directly.
			r, ok := readers[o.tag]
			if ok {
				w, err := o.conn.OpenUniStream()
				if err != nil {
					b.logger.Debug("failed to accept a uniStream", "error", err)
					continue
				}
				go b.copyWithLog(o.tag, w, r, b.logger)
				continue
			}
			// if the writer opener is not registered,
			// store the observer and waiting the writer be registered.
			m, ok := observers[o.tag]
			if !ok {
				observers[o.tag] = map[string]UniStreamConnection{
					o.conn.ID(): o.conn,
				}
			} else {
				m[o.conn.ID()] = o.conn
			}
		case r := <-b.readerChan:
			// if there donot have any observers,
			// store the reader for waiting comming observer to observe it.
			vv, ok := observers[r.tag]
			if !ok {
				_, ok := readers[r.tag]
				if !ok {
					// if there donot has an old writer, store it.
					readers[r.tag] = r.r
				} else {
					// if there has an old writer, close the new comming.
					r.r.Close()
					b.logger.Warn("duplicate writer, close current writer", "tag", r.tag)
				}
				continue
			}

			// if there has observers, copy the writer to them.
			ws := make([]io.Writer, 0)
			for _, opener := range vv {
				w, err := opener.OpenUniStream()
				if err != nil {
					b.logger.Debug("failed to accept a uniStream", "error", err)
					delete(vv, opener.ID())
					break
				}
				// one observer can only observe once.
				delete(vv, opener.ID())

				ws = append(ws, w)
			}
			go b.copyWithLog(r.tag, io.MultiWriter(ws...), r.r, b.logger)
		case tag := <-b.readEOFChan:
			delete(readers, tag)
		}
	}
}

func (b *Broker) copyWithLog(tag string, dst io.Writer, src io.Reader, logger *slog.Logger) {
	_, err := io.Copy(dst, src)
	if err != nil {
		if err == io.EOF {
			b.readEOFChan <- tag
			logger.Debug("writing to all observers has been completed.")
		} else {
			logger.Debug("failed to write a uniStream", "error", err)
		}
	}
}

// Observer is responsible for handling tagged streams.
type Observer interface {
	// Handle is the function responsible for handling tagged streams and writing to a new peer stream.
	// The `r` parameter is used to read data from the tagged stream, and the `w` parameter is used to write data to a new peer stream.
	Handle(r io.Reader, w io.Writer)
}

// ObserveHandleFunc handles tagged streams.
type ObserveHandleFunc func(r io.Reader, w io.Writer)

// Handle calls ObserveHandleFunc itself.
func (f ObserveHandleFunc) Handle(r io.Reader, w io.Writer) { f(r, w) }

// UniStreamConnection opens and accepts uniStream.
type UniStreamConnection interface {
	// ID returns the ID of the connection.
	ID() string
	// OpenUniStream opens uniStream.
	OpenUniStream() (io.WriteCloser, error)
	// AcceptUniStream accepts uniStream.
	AcceptUniStream(context.Context) (io.ReadCloser, error)
}

// UniStreamPeerConnection opens and accepts uniStreams,
// Adding a new method for requesting observe a tag. just work for peer side.
type UniStreamPeerConnection interface {
	// basic connection.
	UniStreamConnection
	// RequestObserve requests server to observe stream be tagged in the specified tag.
	RequestObserve(tag string) error
}

type taggedReader struct {
	tag string
	r   io.ReadCloser
}

type taggedConnection struct {
	tag  string
	conn UniStreamConnection
}
