package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
)

const BROKER_URL = "http://localhost:8080"

func publish(queueId, message string) error {
	resp, err := http.PostForm(BROKER_URL+"/queues/"+queueId+"/messages", url.Values{"body": {message}})
	if err != nil {
		log.Println("Error sending request to broker", err.Error())
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	log.Println("Message Published")
	log.Println(string(body))
	return nil
}

func main() {
	args := os.Args[1:]
	if len(args) != 2 {
		fmt.Println("Usage: publisher.exe <queue_id> <message>")
		return
	}
	queueId := args[0]
	message := args[1]
	err := publish(queueId, message)
	if err != nil {
		log.Println(err.Error())
	}
}
