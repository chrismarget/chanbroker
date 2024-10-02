package broker_test

import (
	"log"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/chrismarget/chanbroker"
	"github.com/stretchr/testify/require"
)

// Subscribers First
// No Subscriber Cancel
// No Drain
// No Stop
func TestBroker_SF_NSC_ND_NS(t *testing.T) {
	testSubscriberCount := 25
	testMessageCount := 1000

	// create the broker
	ch := make(chan int)
	broker := broker.NewBroker(ch)

	// this wait group ensures the sender doesn't begin prematurely
	senderDelayWg := new(sync.WaitGroup)
	senderDelayWg.Add(testSubscriberCount)

	// this wait group ensures the test doesn't end prematurely
	subscriberWg := new(sync.WaitGroup)
	subscriberWg.Add(testSubscriberCount)

	// fire up the subscribers
	sentMessages := make([]int, testMessageCount)
	for i := range testSubscriberCount {
		go func() {
			// create a subscription
			subscriberChan, _ := broker.Subscribe(true, 0)

			// signal that the sender may begin
			senderDelayWg.Done()

			// read and save messages until the channel closes
			var receivedMessages []int
			for msg := range subscriberChan {
				receivedMessages = append(receivedMessages, msg)
			}

			// check and log the results
			require.Equal(t, sentMessages, receivedMessages, "subscriber %d got different messages than expected", i)
			log.Printf("subscriber %d validated %d messages", i, len(receivedMessages))

			// signal that the test may exit
			subscriberWg.Done()
		}()
	}

	// fire up the broker
	senderDelayWg.Wait()
	broker.Start()

	// send messages into the broker
	for i := range testMessageCount {
		msg := rand.Int()
		ch <- msg
		sentMessages[i] = msg
	}
	close(ch)

	// wait for the subscribers to wrap up
	subscriberWg.Wait()
}

// Subscribers Second
// No Subscriber Cancel
// No Drain
// No Stop
func TestBroker_SS_NSC_ND_NS(t *testing.T) {
	testSubscriberCount := 25
	testMessageCount := 100
	messageDelayMs := 10

	// create the broker
	ch := make(chan int)
	broker := broker.NewBroker(ch)

	// this wait group ensures the test doesn't end prematurely
	subscriberWg := new(sync.WaitGroup)
	subscriberWg.Add(testSubscriberCount)

	// fire up the broker
	broker.Start()

	// send messages into the broker
	sentMessages := make([]int, testMessageCount)
	go func() {
		for i := range testMessageCount {
			time.Sleep(time.Duration(messageDelayMs) * time.Millisecond)
			msg := rand.Int()
			ch <- msg
			sentMessages[i] = msg
		}
		close(ch)
	}()

	for i := range testSubscriberCount {
		go func() {
			// miss up to half-ish of the messages
			missMsgCount := rand.IntN(testMessageCount / 2)
			time.Sleep(time.Duration(missMsgCount*messageDelayMs) * time.Millisecond)

			// create a subscription
			subscriberChan, cancelFunc := broker.Subscribe(true, 0)
			_ = cancelFunc

			// read and save messages until the channel closes
			var receivedMessages []int
			for msg := range subscriberChan {
				receivedMessages = append(receivedMessages, msg)
			}

			receivedMsgCount := len(receivedMessages)
			sendMsgCount := len(sentMessages)

			// check and log the results
			require.Equal(t, sentMessages[sendMsgCount-receivedMsgCount:], receivedMessages, "subscriber %d got different messages than expected", i)
			log.Printf("subscriber %d validated %d messages", i, receivedMsgCount)

			// the test may exit
			subscriberWg.Done()
		}()
	}

	// wait for the subscribers to wrap up
	subscriberWg.Wait()
}
