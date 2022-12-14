package rediswatcher

import (
	"time"

	"github.com/go-redis/redis/v8"
)

type WatcherOptions struct {
	Channel            string
	PubConn            *redis.Client
	SubConn            *redis.Client
	Username           string
	Password           string
	Protocol           string
	IgnoreSelf         bool
	LocalID            string
	RecordMetrics      func(*WatcherMetrics)
	SquashMessages     bool
	SquashTimeoutShort time.Duration
	SquashTimeoutLong  time.Duration
	callbackPending    bool
}

type WatcherOption func(*WatcherOptions)

func Channel(subject string) WatcherOption {
	return func(options *WatcherOptions) {
		options.Channel = subject
	}
}

func Username(username string) WatcherOption {
	return func(options *WatcherOptions) {
		options.Username = username
	}
}

func Password(password string) WatcherOption {
	return func(options *WatcherOptions) {
		options.Password = password
	}
}

func Protocol(protocol string) WatcherOption {
	return func(options *WatcherOptions) {
		options.Protocol = protocol
	}
}

func WithRedisSubConnection(connection *redis.Client) WatcherOption {
	return func(options *WatcherOptions) {
		options.SubConn = connection
	}
}

func WithRedisPubConnection(connection *redis.Client) WatcherOption {
	return func(options *WatcherOptions) {
		options.PubConn = connection
	}
}

func LocalID(id string) WatcherOption {
	return func(options *WatcherOptions) {
		options.LocalID = id
	}
}

func IgnoreSelf(ignore bool) WatcherOption {
	return func(options *WatcherOptions) {
		options.IgnoreSelf = ignore
	}
}

func SquashMessages(squash bool) WatcherOption {
	return func(options *WatcherOptions) {
		options.SquashMessages = squash
	}
}

func RecordMetrics(callback func(*WatcherMetrics)) WatcherOption {
	return func(options *WatcherOptions) {
		options.RecordMetrics = callback
	}
}

func SquashTimeoutShort(d time.Duration) WatcherOption {
	return func(options *WatcherOptions) {
		options.SquashTimeoutShort = d
	}
}

func SquashTimeoutLong(d time.Duration) WatcherOption {
	return func(options *WatcherOptions) {
		options.SquashTimeoutLong = d
	}
}

// IsCallbackPending
func IsCallbackPending(w *Watcher, shouldClear bool) bool {
	r := w.options.callbackPending
	if shouldClear {
		w.options.callbackPending = false
	}
	return r
}
