package main

type Message struct {
	Id        string
	Timestamp int
	Body      string
}

type Consumer struct {
	Id          string
	CallbackURL string
}

type Queue struct {
	Id        string
	Name      string
	Consumers []Consumer
}

type Broker struct {
	queues map[string]Queue
}
