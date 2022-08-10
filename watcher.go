package rediswatcher

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/casbin/casbin/v2/persist"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gotomicro/ego/core/elog"
)

type Watcher struct {
	ctx        context.Context
	options    WatcherOptions
	pubConn    *redis.Client
	subConn    *redis.Client
	callback   func(string)
	closed     chan struct{}
	messagesIn chan *redis.Message
	once       sync.Once
}

type WatcherMetrics struct {
	Name        string
	LatencyMs   float64
	LocalID     string
	Channel     string
	Protocol    string
	Error       error
	MessageSize int64
}

const (
	RedisCloseMetric        = "RedisClose"
	RedisDialMetric         = "RedisDial"
	PubSubPublishMetric     = "PubSubPublish"
	PubSubReceiveMetric     = "PubSubReceive"
	PubSubSubscribeMetric   = "PubSubSubscribe"
	PubSubUnsubscribeMetric = "PubSubUnsubscribe"
)

const (
	defaultShortMessageInTimeout = 1 * time.Millisecond
	defaultLongMessageInTimeout  = 1 * time.Minute
)

// NewWatcher creates a new Watcher to be used with a Casbin enforcer
// addr is a redis target string in the format "host:port"
// setters allows for inline WatcherOptions
func NewWatcher(ctx context.Context, addr string, setters ...WatcherOption) (persist.Watcher, error) {
	w := &Watcher{
		ctx:        ctx,
		closed:     make(chan struct{}),
		messagesIn: make(chan *redis.Message),
	}

	w.options = WatcherOptions{
		Channel:            "/casbin",
		Protocol:           "tcp",
		LocalID:            uuid.New().String(),
		SquashTimeoutShort: defaultShortMessageInTimeout,
		SquashTimeoutLong:  defaultLongMessageInTimeout,
	}

	for _, setter := range setters {
		setter(&w.options)
	}

	if err := w.connect(addr); err != nil {
		return nil, err
	}

	// call destructor when the object is released
	runtime.SetFinalizer(w, finalizer)

	w.messageInProcessor()

	go func() {
		for {
			select {
			case <-w.closed:
				return
			default:
				err := w.connect(addr)
				if err == nil {
					err = w.subscribe()
				}
				if err != nil {
					fmt.Printf("Failure from Redis subscription: %v\n", err)
				}
				time.Sleep(2 * time.Second)
			}
		}
	}()

	return w, nil
}

// NewPublishWatcher return a Watcher only publish but not subscribe
func NewPublishWatcher(ctx context.Context, addr string, setters ...WatcherOption) (persist.Watcher, error) {
	w := &Watcher{
		ctx:    ctx,
		closed: make(chan struct{}),
	}

	w.options = WatcherOptions{
		Channel:            "/casbin",
		Protocol:           "tcp",
		LocalID:            uuid.New().String(),
		SquashTimeoutShort: defaultShortMessageInTimeout,
		SquashTimeoutLong:  defaultLongMessageInTimeout,
	}

	for _, setter := range setters {
		setter(&w.options)
	}

	if err := w.connect(addr); err != nil {
		return nil, err
	}

	// call destructor when the object is released
	runtime.SetFinalizer(w, finalizer)

	return w, nil
}

// SetUpdateCallback sets the update callback function invoked by the watcher
// when the policy is updated. Defaults to Enforcer.LoadPolicy()
func (w *Watcher) SetUpdateCallback(callback func(string)) error {
	w.callback = callback
	return nil
}

// Update publishes a message to all other casbin instances telling them to
// invoke their update callback
func (w *Watcher) Update() error {
	startTime := time.Now()
	elog.Info("Casbin update", elog.String("channel", w.options.Channel), elog.String("LocalID", w.options.LocalID))
	if _, err := w.pubConn.Publish(w.ctx, w.options.Channel, w.options.LocalID).Result(); err != nil {
		if w.options.RecordMetrics != nil {
			w.options.RecordMetrics(w.createMetrics(PubSubPublishMetric, startTime, err))
		}
		return err
	}
	if w.options.RecordMetrics != nil {
		w.options.RecordMetrics(w.createMetrics(PubSubPublishMetric, startTime, nil))
	}

	return nil
}

// Close disconnects the watcher from redis
func (w *Watcher) Close() {
	finalizer(w)
}

func (w *Watcher) connect(addr string) error {
	if w.pubConn == nil {
		if err := w.connectPub(addr); err != nil {
			return err
		}
	}
	if w.subConn == nil {
		if err := w.connectSub(addr); err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) connectPub(addr string) error {
	if w.options.PubConn != nil {
		w.pubConn = w.options.PubConn
		return nil
	}
	c, err := w.dial(addr)
	if err != nil {
		return err
	}
	w.pubConn = c
	return nil
}

func (w *Watcher) connectSub(addr string) error {
	if w.options.SubConn != nil {
		w.subConn = w.options.SubConn
		return nil
	}

	c, err := w.dial(addr)
	if err != nil {
		return err
	}
	w.subConn = c
	return nil
}

func (w *Watcher) dial(addr string) (*redis.Client, error) {
	startTime := time.Now()
	opt := redis.Options{
		DB:       0,
		Addr:     addr,
		Password: w.options.Password,
	}
	if w.options.RecordMetrics != nil {
		w.options.RecordMetrics(w.createMetrics(RedisDialMetric, startTime, nil))
	}
	c := redis.NewClient(&opt)
	if err := c.Ping(context.Background()).Err(); err != nil {
		elog.Panic("start stub redis", elog.FieldErr(err))
	}
	return c, nil
}

func (w *Watcher) unsubscribe(psc *redis.PubSub) {
	startTime := time.Now()
	err := psc.PUnsubscribe(w.ctx)
	if w.options.RecordMetrics != nil {
		w.options.RecordMetrics(w.createMetrics(PubSubUnsubscribeMetric, startTime, err))
	}
}

func (w *Watcher) subscribe() error {
	psc := w.subConn.PSubscribe(w.ctx, w.options.Channel)
	startTime := time.Now()
	if w.options.RecordMetrics != nil {
		w.options.RecordMetrics(w.createMetrics(PubSubSubscribeMetric, startTime, nil))
	}
	defer w.unsubscribe(psc)

	for {
		startTime = time.Now()
		msg, err := psc.ReceiveMessage(w.ctx)
		if err != nil {
			elog.Error("subscribe", elog.String("error", err.Error()))
			if w.options.RecordMetrics != nil {
				w.options.RecordMetrics(w.createMetrics(PubSubReceiveMetric, startTime, err))
			}
			return err
		}
		elog.Info("Casbin subscribe", elog.Any("msg", msg))

		if w.options.RecordMetrics != nil {
			watcherMetrics := w.createMetrics(PubSubReceiveMetric, startTime, nil)
			watcherMetrics.MessageSize = int64(len(msg.Payload))
			w.options.RecordMetrics(watcherMetrics)
		}
		elog.Info("Casbin Message")
		w.messagesIn <- msg

	}
}

func (w *Watcher) messageInProcessor() {
	w.options.callbackPending = false
	var data string
	timeOut := w.options.SquashTimeoutLong
	go func() {
		for {
			select {
			case <-w.closed:
				return
			case msg := <-w.messagesIn:
				if w.callback != nil {
					data = msg.Payload
					switch {
					case !w.options.IgnoreSelf && !w.options.SquashMessages:
						w.callback(data)
					case w.options.IgnoreSelf && data == w.options.LocalID: // ignore message
					case !w.options.IgnoreSelf && w.options.SquashMessages:
						w.options.callbackPending = true
					case w.options.IgnoreSelf && data != w.options.LocalID && !w.options.SquashMessages:
						w.callback(data)
					case w.options.IgnoreSelf && data != w.options.LocalID && w.options.SquashMessages:
						w.options.callbackPending = true
					default:
						w.callback(data)
					}
				}
				if w.options.callbackPending { // set short timeout
					timeOut = w.options.SquashTimeoutShort
				}
			case <-time.After(timeOut):
				if w.options.callbackPending {
					w.options.callbackPending = false
					w.callback(data)                      // data will be last message recieved
					timeOut = w.options.SquashTimeoutLong // long timeout
				}
			}
		}
	}()
}

func (w *Watcher) createMetrics(metricsName string, startTime time.Time, err error) *WatcherMetrics {
	return &WatcherMetrics{
		Name:      metricsName,
		Channel:   w.options.Channel,
		LocalID:   w.options.LocalID,
		Protocol:  w.options.Protocol,
		LatencyMs: float64(time.Since(startTime)) / float64(time.Millisecond),
		Error:     err,
	}
}

// GetWatcherOptions return option settings
func (w *Watcher) GetWatcherOptions() WatcherOptions {
	return w.options
}

func finalizer(w *Watcher) {
	w.once.Do(func() {
		close(w.closed)
		startTime := time.Now()
		err := w.subConn.Close()
		if w.options.RecordMetrics != nil {
			w.options.RecordMetrics(w.createMetrics(RedisCloseMetric, startTime, err))
		}
		startTime = time.Now()
		err = w.pubConn.Close()
		if w.options.RecordMetrics != nil {
			w.options.RecordMetrics(w.createMetrics(RedisCloseMetric, startTime, err))
		}
	})
}
