package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
)

type messageHandlerGreeting struct{}

type Message struct {
	Name      string
	Content   string
	Timestamp string
	Index     int
}

type NSQhandler struct {
	Config  nsq.Config
	Topic   string
	Channel string
	Handler nsq.HandlerFunc
}

func main() {

	var messageHandlerGreeting messageHandlerGreeting

	consumers := []NSQhandler{
		NSQhandler{
			Config: nsq.Config{
				MaxAttempts:         10,
				MaxInFlight:         5,
				MaxRequeueDelay:     time.Second * 900,
				DefaultRequeueDelay: time.Second * 0,
			},
			Topic:   "topic_a",
			Channel: "channel_a",
			Handler: messageHandlerGreeting.HandleNew,
		},
	}
	var qList []*nsq.Consumer

	shuttedDown := 0
	go func() {
		var (
			wg  sync.WaitGroup
			mux sync.Mutex
		)
		total := len(qList)
		wg.Add(total)
		for _, cons := range qList {
			go func(c *nsq.Consumer) { // use goroutines to  stop all of them ASAP
				defer wg.Done()
				c.Stop()
				select {
				case <-c.StopChan:
					mux.Lock()
					shuttedDown++
					mux.Unlock()
				case <-time.After(1 * time.Minute): // wait for at max 1 minute
				}
			}(cons)
		}
	}()

	go func() {
		for _, consumer := range consumers {
			//The only valid way to instantiate the Config
			config := nsq.NewConfig()
			//Tweak several common setup in config
			// Maximum number of times this consumer will attempt to process a message before giving up
			config.MaxAttempts = consumer.Config.MaxAttempts
			// Maximum number of messages to allow in flight
			config.MaxInFlight = consumer.Config.MaxInFlight
			// Maximum duration when REQueueing
			config.MaxRequeueDelay = consumer.Config.MaxRequeueDelay
			config.DefaultRequeueDelay = consumer.Config.DefaultRequeueDelay
			//Init topic name and channel
			topic := consumer.Topic
			channel := consumer.Channel

			//Creating the consumer
			q, err := nsq.NewConsumer(topic, channel, config)
			if err != nil {
				log.Fatal(err)
			}

			// Set the Handler for messages received by this Consumer.
			q.AddHandler(consumer.Handler)

			//Use nsqlookupd to find nsqd instances
			q.ConnectToNSQDs([]string{"127.0.0.1:4150"})

			// wait for signal to exit
			// Gracefully stop the consumer.
			qList = append(qList, q)
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

}

func (h *messageHandlerGreeting) HandleNew(m *nsq.Message) error {
	//Process the Message
	var request Message
	if err := json.Unmarshal(m.Body, &request); err != nil {
		log.Println("Error when Unmarshaling the message body, Err : ", err)
		// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
		return err
	}
	log.Println(fmt.Sprintf("new %d", request.Index))
	m.Finish()
	// Will automatically set the message as finish
	return nil
}
