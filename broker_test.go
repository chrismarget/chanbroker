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

// Blocking Subscribers First
// No Subscriber Cancel
// No Stop
func TestBroker_BSF_NSC_NS(t *testing.T) {
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

// Blocking Subscribers Second
// No Subscriber Cancel
// No Stop
func TestBroker_BSS_NSC_NS(t *testing.T) {
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

// Nonblocking Subscribers First
// No Subscriber Cancel
// No Stop
func TestBroker_NSF_NSC_NS(t *testing.T) {
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
			subscriberChan, _ := broker.Subscribe(false, 0)

			// signal that the sender may begin
			senderDelayWg.Done()

			// read and save messages until the channel closes
			var receivedMessages []int
			for msg := range subscriberChan {
				receivedMessages = append(receivedMessages, msg)
			}

			// check and log the results
			require.NotEqual(t, sentMessages, receivedMessages, "subscriber %d should not have received every message", i)
			log.Printf("subscriber %d got %d of %d messages (message loss is expected)", i, len(receivedMessages), testMessageCount)

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

// Blocking Subscribers First
// No Subscriber Cancel
// No Stop
//
// Single blocking slow client ends subscription halfway through. Ensure that:
// - the slow client provides backpressure on the sender
// - the sender rate skyrockets after the subscriber cancels
func TestBroker_BSF_NSC_D_NS(t *testing.T) {
	testMessageCount := 1000
	slowClientMessageCount := testMessageCount / 2
	subscriberDelayMs := 1

	// create the broker
	ch := make(chan int)
	broker := broker.NewBroker(ch)

	// this wait group ensures the test doesn't begin prematurely
	subscriberStartWg := new(sync.WaitGroup)
	subscriberStartWg.Add(1)

	// this wait group ensures the test doesn't end prematurely
	subscriberDoneWg := new(sync.WaitGroup)
	subscriberDoneWg.Add(1)

	var subscriberDone time.Time // filled when the subscriber completes

	// create a subscription; slowly collect half of the messages
	go func() {
		subscriberChan, cancel := broker.Subscribe(true, 1)
		subscriberStartWg.Done()
		i := 0
		for range subscriberChan {
			i++
			time.Sleep(time.Duration(subscriberDelayMs) * time.Millisecond)
			if i == slowClientMessageCount {
				log.Printf("requesting cancelation after %d messages\n", i)
				cancel()
			}
		}

		log.Printf("actual message count: %d\n", i)
		subscriberDone = time.Now()
		subscriberDoneWg.Done()
	}()

	// wait for the subscriber to come online, then start the broker
	subscriberStartWg.Wait()
	broker.Start()

	start := time.Now()
	// send messages into the broker as fast as possible
	for range testMessageCount {
		ch <- rand.Int()
	}
	end := time.Now()
	close(ch)

	// wait for the subscriber to complete (just in case?)
	subscriberDoneWg.Wait()

	subscriberTimeMin := time.Millisecond * time.Duration(subscriberDelayMs*slowClientMessageCount)
	subscriberTimeMax := subscriberTimeMin * 5 / 4
	require.Greaterf(t, subscriberDone.Sub(start), subscriberTimeMin,
		"the subscriber should have taken at least %d, actual: %d", subscriberTimeMin, subscriberDone.Sub(start))
	require.Greaterf(t, subscriberTimeMax, subscriberDone.Sub(start),
		"the subscriber shouldn't have taken more than %d, actual: %d", subscriberTimeMin, subscriberDone.Sub(start))

	require.Greaterf(t, time.Millisecond*time.Duration((testMessageCount-slowClientMessageCount)*subscriberDelayMs/2), end.Sub(subscriberDone),
		"sender with no client should have completed almost instantly, actual: %d", end.Sub(subscriberDone))

	log.Printf("time with slow subscriber online: %s\n", subscriberDone.Sub(start))
	log.Printf("time with no subscriber online: %s\n", end.Sub(subscriberDone))
}
