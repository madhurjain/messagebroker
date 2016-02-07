package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/phayes/freeport"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

const BROKER_URL = "http://localhost:8080"
const CONSUMERS = 5

type NewQueueReponse struct {
	Status string `json:"status"`
	Id     string `json:"id"`
}

/* Utility function to generate unique id */
func generateKey() (uuidHex string) {
	// Generate UUID from timestamp and MAC address and get its MD5
	uuid := md5.Sum(uuid.NewV1().Bytes())
	uuidHex = fmt.Sprintf("%x", uuid)
	return uuidHex
}

func printData(c *gin.Context) {
	callback := c.Param("callback")
	msgId := c.PostForm("id")
	timestamp := c.PostForm("timestamp")
	body := c.PostForm("body")
	log.Println("Message recieved on callback", callback)
	log.Println(msgId, timestamp, body)

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func createQueue() (string, error) {
	queueName := generateKey()
	resp, err := http.PostForm(BROKER_URL+"/queues", url.Values{"name": {queueName}})
	if err != nil {
		log.Println("Error sending request to broker", err.Error())
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	var queueResp NewQueueReponse
	if err := json.Unmarshal(body, &queueResp); err != nil {
		return "", err
	}
	return queueResp.Id, nil
}

func createConsumers(queueId, callbackUrl string) error {
	resp, err := http.PostForm(BROKER_URL+"/queues/"+queueId+"/consumers", url.Values{"callback_url": {callbackUrl}})
	if err != nil {
		log.Println("Error sending request to broker", err.Error())
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	log.Println("Consumer Created")
	log.Println(string(body))
	return nil
}

func main() {

	// Create a new queue
	queueId, err := createQueue()
	if err != nil {
		log.Println("Unable to create queue. Terminating.")
		log.Println("Make sure broker is running")
		return
	}
	log.Println("Queue created on broker with Id", queueId)

	// get available free port
	port := strconv.Itoa(freeport.GetPort())
	callbackBase := "http://localhost:" + port
	for i := 0; i < CONSUMERS; i++ {
		// Create consumers with callback URLs of form http://localhost:port/<1..5>
		err = createConsumers(queueId, callbackBase+"/"+strconv.Itoa(i+1))
		if err != nil {
			log.Println("Unable to create consumer. Terminating.")
			log.Println("Make sure broker is running")
			return
		}
	}

	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "Message Consumer")
	})

	// Receive published messages and print
	router.POST("/:callback", printData)

	// listen and serve on available free port
	log.Println("Starting Consumer on", port)
	router.Run(":" + port)

}
