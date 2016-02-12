package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"strconv"
	"sync"
	"time"
)

type Broker struct {
	queues map[string]*Queue
	sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{queues: make(map[string]*Queue)}
}

func (b *Broker) Broadcast(queueId, body string) error {
	log.Println(" --> broadcast message", queueId, body)
	// check if body is not blank
	if body == "" {
		return errors.New("Please specify a message to be sent")
	}

	b.RLock()
	queue, ok := b.queues[queueId]
	b.RUnlock()

	message := &Message{id: generateKey(), timestamp: strconv.FormatInt(time.Now().Unix(), 10), body: body}
	if ok {
		// send message to all consumers within the queue
		queue.RLock()
		for _, consumer := range queue.consumers {
			job := &Job{consumer: consumer, message: message}
			queue.job <- job
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
	queue := NewQueue(queueName)
	log.Println("-- adding queue", queue.Id)
	b.Lock()
	b.queues[queue.Id] = queue
	b.Unlock()
	return queue.Id, nil
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
		return fmt.Errorf("Queue (%s) does not exist", queueId)
	}
	return nil
}

func (b *Broker) DeleteQueue(queueId string) error {
	// check if queue with specified Id exists, then delete it
	b.Lock()
	defer b.Unlock()
	if _, ok := b.queues[queueId]; ok {
		b.queues[queueId].close()
		delete(b.queues, queueId)
	} else {
		return fmt.Errorf("Queue (%s) does not exist", queueId)
	}
	return nil
}

/* Consumer */
func (b *Broker) GetConsumers(queueId string) ([]Consumer, error) {
	b.RLock()
	defer b.RUnlock()
	if queue, ok := b.queues[queueId]; ok {
		return queue.GetConsumers(), nil
	} else {
		return nil, fmt.Errorf("Queue (%s) does not exist", queueId)
	}
}

func (b *Broker) AddConsumer(queueId, callbackUrl string) (string, error) {
	b.RLock()
	defer b.RUnlock()
	if queue, ok := b.queues[queueId]; ok {
		return queue.AddConsumer(callbackUrl)
	} else {
		return "", fmt.Errorf("Queue (%s) does not exist", queueId)
	}
}

func (b *Broker) RemoveConsumer(queueId, consumerId string) error {
	b.RLock()
	defer b.RUnlock()
	if queue, ok := b.queues[queueId]; ok {
		return queue.RemoveConsumer(consumerId)
	} else {
		return fmt.Errorf("Queue (%s) does not exist", queueId)
	}
}
