package broker

import (
	"context"
	"sync"
)

// A Broker distributes copies of objects received on the input channel
// to all subscriber channels. If the input channel is closed, the Broker
// will close all subscriber channels.
type Broker[T interface{}] struct {
	input            <-chan T
	mutex            *sync.Mutex
	nextSubscriberId uint
	stopFunc         func()
	subscriptions    map[uint]subscription[T]
}

// Start causes the broker to begin reading messages and distributing them
// to subscribers.
func (b *Broker[T]) Start() {
	ctx := context.Background()

	b.mutex.Lock()
	if b.stopFunc != nil {
		panic("attempt to start a running broker")
	}
	ctx, b.stopFunc = context.WithCancel(ctx)
	b.mutex.Unlock()

	go func() {
		for {
			select {
			case _ = <-ctx.Done(): // ignoring cancellation error
				// The context has been cancelled. We are done.
				b.closeSubscriberChannels()
				b.deleteSubscribers()
				return
			case msg, ok := <-b.input:
				if !ok {
					// The input channel has closed. We are done.
					b.closeSubscriberChannels()
					b.deleteSubscribers()
					b.stopFunc() // free up the context
					return
				}

				b.mutex.Lock()
				for _, s := range b.subscriptions {
					s.send(msg)
				}
				b.mutex.Unlock()
			}
		}
	}()
}

// Stop the broker. All subscriber channels will be closed and the subscribers will be deleted.
// No more messages will be read from the input channel.
func (b *Broker[T]) Stop() {
	b.stopFunc()
}

// Subscribe returns a channel and a function which cancels the subscription.
// blocking indicates whether backpressure in this subscription should cause
// backpressure on the origin channel. buffsize indicates the buffer size to
// be used on the subscriber-facing channel.
func (b *Broker[T]) Subscribe(blocking bool, buffSize int) (<-chan T, func()) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	subscriberId := b.nextSubscriberId
	b.nextSubscriberId++

	b.subscriptions[subscriberId] = subscription[T]{
		ch:       make(chan T, buffSize),
		blocking: blocking,
	}

	cancelFunc := func() {
		go func() {
			b.mutex.Lock()
			defer b.mutex.Unlock()

			subscriber, ok := b.subscriptions[subscriberId]
			if !ok {
				// subscriber has been deleted (input channel closure?)
				// before the subscriber got around to requesting it.
				return
			}

			close(subscriber.ch)
			delete(b.subscriptions, subscriberId)
		}()
	}

	return b.subscriptions[subscriberId].ch, cancelFunc
}

func (b *Broker[T]) closeSubscriberChannels() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for _, s := range b.subscriptions {
		s.close()
	}
}

func (b *Broker[T]) deleteSubscribers() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for k := range b.subscriptions {
		delete(b.subscriptions, k)
	}
}

func NewBroker[T interface{}](inputChan <-chan T) Broker[T] {
	return Broker[T]{
		input:         inputChan,
		mutex:         new(sync.Mutex),
		subscriptions: make(map[uint]subscription[T]),
	}
}
