package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Consumer struct {
	Id          string `json:"id"`
	QueueId     string `json:"queue_id"`
	CallbackUrl string `json:"callback_url"`
}

type Queue struct {
	Id        string `json:"id"`
	Name      string `json:"name"`
	consumers map[string]*url.URL
	sync.RWMutex
}

type Broker struct {
	queues map[string]*Queue
	sync.RWMutex
}

type Message struct {
	id        string
	timestamp string
	body      string
}

func NewBroker() *Broker {
	return &Broker{queues: make(map[string]*Queue)}
}

func (b *Broker) Broadcast(queueId, body string) error {
	log.Println(" -- broadcast message", queueId, body)
	// check if body is not blank
	if body == "" {
		return errors.New("Please specify a message to be sent")
	}
	b.RLock()
	queue, ok := b.queues[queueId]
	b.RUnlock()
	if ok {
		queue.RLock()
		message := &Message{id: generateKey(), timestamp: strconv.FormatInt(time.Now().Unix(), 10), body: body}
		for _, callback := range queue.consumers {
			log.Println("Message: ", message, " To: ", callback.String())
			// POST message to consumers
			go func(msg *Message, to string) {
				_, err := http.PostForm(to, url.Values{"id": {msg.id}, "timestamp": {msg.timestamp}, "body": {msg.body}})
				if err != nil {
					log.Println("Error sending request to client", err.Error())
				}
			}(message, callback.String())
		}
		queue.RUnlock()
	} else {
		return fmt.Errorf("Queue (Id %s) does not exist", queueId)
	}
	return nil
}

/* Utility function to generate unique id */
func generateKey() (uuidHex string) {
	// Generate UUID from timestamp and MAC address and get its MD5
	uuid := md5.Sum(uuid.NewV1().Bytes())
	uuidHex = fmt.Sprintf("%x", uuid)
	return uuidHex
}

/* Queue */
func (b *Broker) queueNameExists(queueName string) bool {
	exists := false
	b.RLock()
	defer b.RUnlock()
	for _, queue := range b.queues {
		if queue.Name == queueName {
			exists = true
			break
		}
	}
	return exists
}

func (b *Broker) GetQueues() []Queue {
	b.RLock()
	var queues []Queue
	for _, queue := range b.queues {
		queues = append(queues, Queue{Id: queue.Id, Name: queue.Name})
	}
	b.RUnlock()
	return queues
}

func (b *Broker) AddQueue(queueName string) (string, error) {
	// check if queue name supplied is not blank
	if queueName == "" {
		return "", errors.New("Please specify a name for Queue")
	}
	// check if queue already exists with same name
	if b.queueNameExists(queueName) {
		return "", fmt.Errorf("Queue with name %s already exists", queueName)
	}
	queueId := generateKey()
	log.Println("-- adding queue", queueId)
	b.Lock()
	b.queues[queueId] = &Queue{Id: queueId, Name: queueName, consumers: make(map[string]*url.URL)}
	b.Unlock()
	return queueId, nil
}

func (b *Broker) EditQueue(queueId string, queueName string) error {
	// check if queue name supplied is not blank
	if queueName == "" {
		return errors.New("Please specify a new name for queue")
	}
	// check if queue already exists with same name
	if b.queueNameExists(queueName) {
		return fmt.Errorf("Queue with name %s already exists", queueName)
	}
	// check if queue with specified Id exists and change its name
	b.Lock()
	defer b.Unlock()
	if queue, ok := b.queues[queueId]; ok {
		queue.Name = queueName
	} else {
		return fmt.Errorf("Queue (Id %s) does not exist", queueId)
	}
	return nil
}

func (b *Broker) DeleteQueue(queueId string) error {
	// check if queue with specified Id exists, then delete it
	b.Lock()
	defer b.Unlock()
	if _, ok := b.queues[queueId]; ok {
		delete(b.queues, queueId)
	} else {
		return fmt.Errorf("Queue (Id %s) does not exist", queueId)
	}
	return nil
}

func (b *Broker) GetConsumers(queueId string) ([]Consumer, error) {
	b.RLock()
	defer b.RLock()
	if queue, ok := b.queues[queueId]; ok {
		return queue.GetConsumers(), nil
	} else {
		return nil, fmt.Errorf("Queue (Id %s) does not exist", queueId)
	}
}

func (b *Broker) AddConsumer(queueId, callbackUrl string) (string, error) {
	b.RLock()
	defer b.RUnlock()
	if queue, ok := b.queues[queueId]; ok {
		return queue.AddConsumer(callbackUrl)
	} else {
		return "", fmt.Errorf("Queue (Id %s) does not exist", queueId)
	}
}

func (b *Broker) RemoveConsumer(queueId, consumerId string) error {
	b.RLock()
	defer b.RLock()
	if queue, ok := b.queues[queueId]; ok {
		return queue.RemoveConsumer(consumerId)
	} else {
		return fmt.Errorf("Queue (Id %s) does not exist", queueId)
	}
}

/* Consumer */
func (q *Queue) GetConsumers() []Consumer {
	q.RLock()
	var consumers []Consumer
	for id, url := range q.consumers {
		consumers = append(consumers, Consumer{Id: id, QueueId: q.Id, CallbackUrl: url.String()})
	}
	q.RUnlock()
	return consumers
}

func (q *Queue) AddConsumer(callbackUrl string) (string, error) {
	u, err := url.Parse(callbackUrl)
	if err != nil {
		return "", err
	}
	consumerId := generateKey()
	q.Lock()
	log.Println("-- adding consumer with callback", callbackUrl)
	q.consumers[consumerId] = u
	q.Unlock()
	return consumerId, nil
}

func (q *Queue) RemoveConsumer(id string) error {
	q.Lock()
	defer q.Unlock()
	if _, ok := q.consumers[id]; ok {
		delete(q.consumers, id)
	} else {
		return fmt.Errorf("Consumer (id %s) Not Found", id)
	}
	return nil
}
