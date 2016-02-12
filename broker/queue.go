package main

import (
	"container/heap"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// If message delivery fails for a consumer, it will be retried n times
// with retry times added in the below slice. time is in seconds
var RetryTimes = [...]uint16{5, 10, 15, 20}

type Message struct {
	id        string
	timestamp string
	body      string
}

type Consumer struct {
	Id          string `json:"id"`
	QueueId     string `json:"queue_id"`
	CallbackUrl string `json:"callback_url"`
}

type Job struct {
	consumer      *Consumer
	message       *Message
	retryAtTime   time.Time
	retryDuration time.Duration
	retryCount    uint8
}

type Queue struct {
	Id         string `json:"id"`
	Name       string `json:"name"`
	job        chan *Job
	retry      chan *Job
	stop       chan bool
	consumers  map[string]*Consumer
	retryTimer *RetryTimer
	retryQueue RetryQueue
	sync.RWMutex
}

func NewQueue(queueName string) *Queue {
	queue := &Queue{Id: generateKey(), Name: queueName, job: make(chan *Job), retry: make(chan *Job), stop: make(chan bool), retryTimer: NewRetryTimer(), consumers: make(map[string]*Consumer)}
	go queue.process()
	return queue
}

// process method starts as soon as the queue is added and terminates when the queue is deleted
// each case in the select is executed whenever there is new data available on the associated channel
func (q *Queue) process() {
	log.Println("Running Process for Queue", q.Name)
	for {
		select {
		case job := <-q.job: // send message to consumer
			go q.send(job.consumer, job.message, job.retryCount)
		case retryJob := <-q.retry: // add job to retry queue and set the retry timer
			heap.Push(&q.retryQueue, retryJob)
			// if retry timer is not set or elapsed
			if !q.retryTimer.IsRunning() {
				// retry timer not running either means this is the first retry job being added
				// or all the past retry jobs in queue are done
				q.retryTimer.SetDuration(retryJob.retryDuration)
			} else {
				// if timer is running, but next scheduled time is more than 2 seconds
				// of the job just added to queue, rescheudle it to run it earlier
				if q.retryTimer.retryTime.Sub(retryJob.retryAtTime) > (2 * time.Second) {
					q.retryTimer.SetTime(retryJob.retryAtTime)
				}
			}
		case <-q.retryTimer.Event: // retry timer expired
			// fetch jobs in retry queue and push them
			// to the job channel to be processed
			now := time.Now()
			queueLen := q.retryQueue.Len()
			q.RLock()
			// get all retry jobs from the queue based on priority
			for i := 0; i < queueLen; i++ {
				retryJob := q.retryQueue.Peek().(*Job)
				// process only the jobs for which retry time is within 2 seconds from now
				if retryJob.retryAtTime.Sub(now) < (2 * time.Second) {
					heap.Pop(&q.retryQueue)
					// if consumer was deleted. skip sending message to it.
					if _, ok := q.consumers[retryJob.consumer.Id]; ok {
						// retry sending message to this consumer
						go func(job *Job) {
							q.job <- job
						}(retryJob)
					}
				} else {
					break
				}
			}
			q.RUnlock()
			// set retry timer to the retry time of next job in priority queue
			if q.retryQueue.Len() > 0 {
				q.retryTimer.SetTime(q.retryQueue.Peek().(*Job).retryAtTime)
			}
		case <-q.stop: // queue being deleted. stop gracefully.
			log.Println("Terminating Process for Queue", q.Name)
			return
		}
	}
}

// make a post request to consumer's callback url sending the message body
// if an error occurs, add it to the retry queue
func (q *Queue) send(consumer *Consumer, message *Message, retryCount uint8) {
	log.Printf("--> sending message to consumer %v\n", consumer.Id)
	_, err := http.PostForm(consumer.CallbackUrl, url.Values{"id": {message.id}, "timestamp": {message.timestamp}, "body": {message.body}})
	if err != nil {
		// message was not sent. retry exponentially.
		retryCount++
		if retryCount > uint8(len(RetryTimes)) {
			log.Printf("maximum number of retries elapsed for consumer %v. Message discarded.\n", consumer.Id)
			return
		}
		log.Printf("--> error sending message to consumer %v\n", consumer.Id)
		// create a new retry job and add it to the retry queue
		// retry duration is set based on number of past attempts. exponential back-off
		retryDuration := time.Duration(RetryTimes[retryCount-1]) * time.Second
		retryJob := &Job{consumer: consumer, message: message, retryAtTime: time.Now().Add(retryDuration), retryDuration: retryDuration, retryCount: retryCount}
		q.retry <- retryJob
	}
}

// called when the queue is deleted
// stops the process loop gracefully
func (q *Queue) close() {
	q.stop <- true
}

/* Consumer methods called by broker */
func (q *Queue) GetConsumers() []Consumer {
	q.RLock()
	var consumers []Consumer
	for id, consumer := range q.consumers {
		consumers = append(consumers, Consumer{Id: id, QueueId: q.Id, CallbackUrl: consumer.CallbackUrl})
	}
	q.RUnlock()
	return consumers
}

func (q *Queue) AddConsumer(callbackUrl string) (string, error) {
	u, err := url.Parse(callbackUrl)
	if err != nil {
		return "", err
	}
	q.Lock()
	log.Println("--> adding consumer with callback", callbackUrl)
	consumer := &Consumer{Id: generateKey(), QueueId: q.Id, CallbackUrl: u.String()}
	q.consumers[consumer.Id] = consumer
	q.Unlock()
	return consumer.Id, nil
}

func (q *Queue) RemoveConsumer(id string) error {
	q.Lock()
	defer q.Unlock()
	if _, ok := q.consumers[id]; ok {
		q.consumers[id] = nil
		delete(q.consumers, id)
	} else {
		return fmt.Errorf("Consumer (%s) Not Found", id)
	}
	return nil
}
