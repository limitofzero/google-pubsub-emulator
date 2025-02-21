package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/pubsub"
)

type CreateTopicRequest struct {
	ProjectID string `json:"projectId"`
	TopicID   string `json:"topicId"`
}

type PublishMessageRequest struct {
	Data       json.RawMessage   `json:"data"`
	Attributes map[string]string `json:"attributes"`
	ProjectID  string            `json:"projectId"`
	TopicID    string            `json:"topicId"`
}

type CreateSubscriptionRequest struct {
	ProjectID      string `json:"projectId"`
	TopicID        string `json:"topicId"`
	SubscriptionID string `json:"subscriptionId"`
}

func createTopicHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method is not supported", http.StatusMethodNotAllowed)
		return
	}

	var createTopicRequest CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&createTopicRequest); err != nil {
		http.Error(w, "Invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	topic, err := createTopic(createTopicRequest.ProjectID, createTopicRequest.TopicID)
	if err != nil {
		http.Error(w, "Failed to create topic: "+err.Error(), http.StatusInternalServerError)
	}

	fmt.Fprintf(w, "Topic %s was successfully created", topic.ID())
}

func publishMessageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method is not supported", http.StatusMethodNotAllowed)
		return
	}

	var req PublishMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, req.ProjectID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Client creation error: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	topic := client.Topic(req.TopicID)
	result := topic.Publish(ctx, &pubsub.Message{
		Data:       req.Data,
		Attributes: req.Attributes,
	})

	id, err := result.Get(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error when msg was published: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Msg was published with id %s", id)
}

func createSubscriptionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	err := createTopicAndSub(req.ProjectID, req.TopicID, []string{req.SubscriptionID})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create subscription: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Subscription %s successfully created for topic %s", req.SubscriptionID, req.TopicID)
}

func createTopicAndSub(projectId string, topicId string, subs []string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	defer client.Close()

	topic := client.Topic(topicId)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		topic, err = client.CreateTopic(ctx, topicId)
		if err != nil {
			return err
		}
		fmt.Println("Topic successfully created ", topic.ID())
	} else {
		fmt.Println("Topic already exists ", topic.ID())
	}

	for _, subId := range subs {
		sub := client.Subscription(subId)
		exists, err := sub.Exists(ctx)
		if err != nil {
			return err
		}

		if !exists {
			sub, err := client.CreateSubscription(ctx, subId, pubsub.SubscriptionConfig{
				Topic: topic,
			})
			if err != nil {
				return err
			}

			fmt.Println("Subscription %s successfully created for topic %s", sub.ID(), topic.ID())
		} else {
			fmt.Println("Subscription %s already exists", sub.ID())
		}

	}

	return nil
}

func createTopic(projectId string, topicId string) (*pubsub.Topic, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	topic := client.Topic(topicId)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		topic, err := client.CreateTopic(ctx, topicId)
		if err != nil || topic == nil {
			return nil, err
		} else {
			fmt.Println("Topic %s successfully created", topic.ID())
			return topic, nil
		}
	}

	fmt.Println("Topic %s already exists", topic.ID())
	return topic, nil
}

func main() {
	// PROJECTID,TOPIC1,TOPIC2:SUBSCRIPTION1:SUBSCRIPTION2,TOPIC3:SUBSCRIPTION3
	topicsAndSubs := os.Getenv("TOPICS_AND_SUBSCRIPTIONS")

	if len(topicsAndSubs) != 0 {
		parsed := strings.Split(topicsAndSubs, ",")

		projectId := parsed[0]
		for i := 1; i < len(parsed); i++ {
			topicAndSubs := strings.Split(parsed[i], ":")
			topicId := topicAndSubs[0]
			if len(topicAndSubs) > 1 {
				subs := strings.Split(topicAndSubs[1], ":")
				err := createTopicAndSub(projectId, topicId, subs)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				_, err := createTopic(projectId, topicId)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}

	http.HandleFunc("/create-topic", createTopicHandler)
	http.HandleFunc("/publish", publishMessageHandler)
	http.HandleFunc("/create-subscription", createSubscriptionHandler)

	port := "3030"
	fmt.Println("Server is started on: " + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
