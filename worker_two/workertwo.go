// Author Vitaliy Zhhuta PoC
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

const subscriptonName = "workertwo" // subcritopn name

const PubsubTopicID = "one" // read topic

type eventMessage struct {
	name    string
	message string
}

var (
	countMu sync.Mutex
	count   int

	subscription *pubsub.Subscription
	// we export this clint for global propouse
	PubsubClient *pubsub.Client //  global
)

func configurePubsub(projectID string) (*pubsub.Client, error) {
	//For beginign we have to configure PubSub.Clinet base on our PROJECT_ID
	ctx := context.Background()
	//Creating a new clinent
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	//Create topic if it's not exit.
	if exists, err := client.Topic(PubsubTopicID).Exists(ctx); err != nil {
		return nil, err
	} else if !exists {
		if _, err := client.CreateTopic(ctx, PubsubTopicID); err != nil {
			return nil, err
		}
	}
	return client, err
}

func subcribe() {
	ctx := context.Background()
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var event eventMessage
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			log.Printf("can't decode message data %v", msg)
			msg.Ack()
			return
		}
		log.Printf("Processing [%s]", event.name)
		if err := updateDB(event); err != nil {
			log.Printf("Event [%s] can't be published; error:%v ", event.name, err)
			msg.Nack()
			return
		}
		countMu.Lock()
		count++
		countMu.Unlock()

		msg.Ack()
		log.Printf("ID [%d] ACK", event.name)
	})
	if err != nil {
		log.Fatal(err)
	}

}
func updateDB(event eventMessage) error {
	// Implement writing to MySQL DB
	return nil
}

// This function should be changed
/*func publishNext(event eventMessage) error {
	ctx := context.Background()
	PubsubClient, err := configurePubsub("riverlife-197216")
	/*if PubsubClient == nil {
		PubsubClient, err := configurePubsub("riverlife-197216")
		if err != nil {
			log.Fatal("Fail to configure PubSub clietn")
			return errors.New("misssing pubsubclient")
		}
		//return errors.New("misssing pubsubclient")
	}
	topic := PubsubClient.Topic(PubsubTopicIDnext)
	exists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatal("Error cheking for topic %v", err)
	}
	if !exists {
		if _, err := PubsubClient.CreateTopic(ctx, PubsubTopicIDnext); err != nil {
			log.Fatal("Failed to create Topic: %v", err)
		}
	}
	d, err := json.Marshal(event)
	if err != nil {
		return err
	}

	servID, err := topic.Publish(ctx, &pubsub.Message{Data: d}).Get(ctx)
	if err != nil {
		return err
	}
	log.Printf("Even havs been published to %s with ID: %s", PubsubTopicIDnext, servID)
	return nil
}*/

func main() {
	ctx := context.Background()

	PubsubClient, err := configurePubsub("PROJECT_ID")
	if err != nil {
		log.Fatalf("Issue during configuring pubsub: error: %v", err)
	}

	topic := PubsubClient.Topic(PubsubTopicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatal("Error cheking for topic %v", err)
	}
	if !exists {
		if _, err := PubsubClient.CreateTopic(ctx, PubsubTopicID); err != nil {
			log.Fatal("Failed to create Topic: %v", err)
		}
	}
	subscription = PubsubClient.Subscription(subscriptonName)
	exists, err = subscription.Exists(ctx)
	if err != nil {
		log.Fatal("Error creating subscription: %v", err)
	}
	if !exists {
		if _, err := PubsubClient.CreateSubscription(ctx, subscriptonName, pubsub.SubscriptionConfig{Topic: topic}); err != nil {
			log.Fatal("Faile to create subcription: %v", err)
		}
	}
	// Start Subcription
	go subcribe()

	// [START http]
	// Publish a count of processed requests to the server homepage.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		countMu.Lock()
		defer countMu.Unlock()
		fmt.Fprintf(w, "This worker has processed %d events.", count)
	})

	port := "8080"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	log.Fatal(http.ListenAndServe(":"+port, nil))
	// [END http]
}
