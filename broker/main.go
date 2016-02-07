package main

import (
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
)

var broker *Broker

func getQueues(c *gin.Context) {
	// return list of queues - id, name
	c.JSON(http.StatusOK, broker.GetQueues())
}

func addQueue(c *gin.Context) {
	// create a new queue - name
	// return status ok and id
	queueName := c.PostForm("name")
	queueId, err := broker.AddQueue(queueName)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "id": queueId})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
	}
}

func editQueue(c *gin.Context) {
	// edit existing queue - name
	// return status ok
	queueId := c.Param("id")
	queueName := c.PostForm("name")
	err := broker.EditQueue(queueId, queueName)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
	}
}

func removeQueue(c *gin.Context) {
	// delete queue object
	// return status ok
	queueId := c.Param("id")
	err := broker.DeleteQueue(queueId)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
	}
}

func publishMessage(c *gin.Context) {
	// send message to all consumers in a queue
	// return status ok
	queueId := c.Param("id")
	message := c.PostForm("body")
	err := broker.Broadcast(queueId, message)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
	}
}

func addConsumer(c *gin.Context) {
	// add consumer with callback_url
	queueId := c.Param("id")
	callbackUrl := c.PostForm("callback_url")
	consumerId, err := broker.AddConsumer(queueId, callbackUrl)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "id": consumerId})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
	}
}

func getConsumers(c *gin.Context) {
	// return list of consumers - id, queue_id, callback_url
	queueId := c.Param("id")
	consumers, err := broker.GetConsumers(queueId)
	if err == nil {
		c.JSON(http.StatusOK, consumers)
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
	}
}

func removeConsumer(c *gin.Context) {
	// delete existing consumer
	// return status ok
	queueId := c.Param("id")
	consumerId := c.Param("consumer_id")
	err := broker.RemoveConsumer(queueId, consumerId)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": err.Error()})
	}
}

func main() {

	broker = NewBroker()

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

	log.Println("Starting Broker on 8080")

	// listen and serve on 0.0.0.0:8080
	router.Run()

}
