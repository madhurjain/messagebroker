package main

import (
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
)

func getQueues(c *gin.Context) {
	log.Println("getQueues")
	// return list of queues - id, name
}

func addQueue(c *gin.Context) {
	// create a new queue - name
	// return status ok and id
}

func editQueue(c *gin.Context) {
	// edit existing queue - name
	// return status ok
}

func removeQueue(c *gin.Context) {
	// delete queue object
	// return status ok
}

func publishMessage(c *gin.Context) {
	// send message to all consumers in a queue
	// return status ok
}

func addConsumer(c *gin.Context) {
	// add consumer with callback_url
}

func getConsumers(c *gin.Context) {
	log.Println("getConsumers")
	// return list of consumers - id, queue_id, callback_url
}

func removeConsumer(c *gin.Context) {
	// delete existing consumer
	// return status ok
}

func main() {
	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "Message Broker API")
	})
	router.GET("/queues", getQueues)
	router.POST("/queues", addQueue)
	router.PUT("/queues/:id", editQueue)
	router.DELETE("/queues/:id", removeQueue)

	router.POST("/queues/:id/messages", publishMessage)

	router.GET("/queues/:id/consumers", getConsumers)
	router.POST("/queues/:id/consumers", addConsumer)
	router.POST("/queues/:id/consumers/:consumer_id", removeConsumer)

	// listen and serve on 0.0.0.0:8080
	router.Run()

}
