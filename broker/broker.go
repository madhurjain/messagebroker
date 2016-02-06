package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"net/url"
	"sync"
	"time"
)

type Consumer struct {
	Id          string
	QueueId     string
	CallbackUrl string
}

type Queue struct {
	Id        string
	Name      string
	consumers map[string]*url.URL
	sync.RWMutex
}

type Broker struct {
	queues map[string]*Queue
	sync.RWMutex
}

type Message struct {
	id        string
	timestamp int64
	body      string
}

func NewBroker() *Broker {
	return &Broker{queues: make(map[string]*Queue)}
}

func (b *Broker) Broadcast(queueId, message string) error {

	// check if message is not blank
	if message == "" {
		return errors.New("Please specify a message to be sent")
	}

	b.Lock()
	defer b.Unlock()
	// check if queue exists
	if queue, ok := b.queues[queueId]; ok {
		queue.Lock()
		message := &Message{id: generateKey(), timestamp: time.Now().Unix(), body: message}
		for _, url := range b.queues[queueId].consumers {
			log.Println("Message: ", message, " To: ", url.String())
			// POST message to consumers

		}
		queue.Unlock()
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
	for _, queue := range b.queues {
		if queue.Name == queueName {
			exists = true
			break
		}
	}
	b.RUnlock()
	return exists
}

func (b *Broker) GetQueues() []Queue {
	b.RLock()
	queues := make([]Queue, len(b.queues))
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

/* Consumer */
func (q *Queue) GetConsumers() []Consumer {
	q.RLock()
	consumers := make([]Consumer, len(q.consumers))
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
