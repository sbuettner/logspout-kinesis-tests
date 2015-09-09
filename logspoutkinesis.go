// Based on:
// - https://github.com/rtoma/logspout-redis-logstash/blob/master/redis.go

package logspoutkinesis

import (
    "fmt"
    "strings"
    "log"
    "os"
    "time"
    "encoding/json"
    "github.com/gliderlabs/logspout/router"
)

type KinesisAdapter struct {
    route       *router.Route
    key         string
    docker_host string
    use_v0      bool
}

type DockerFields struct {
    Name        string `json:"name"`
    CID         string `json:"cid"`
    Image       string `json:"image"`
    ImageTag    string `json:"image_tag,omitempty"`
    Source      string `json:"source"`
    DockerHost  string `json:"docker_host,omitempty"`
}

type LogstashFields struct {
    Docker      DockerFields    `json:"docker"`
}

type LogstashMessageV0 struct {
    Timestamp   string            `json:"@timestamp"`
    Sourcehost  string            `json:"@source_host"`
    Message     string            `json:"@message"`
    Fields      LogstashFields    `json:"@fields"`
}

type LogstashMessageV1 struct {
    Timestamp   string            `json:"@timestamp"`
    Sourcehost  string            `json:"host"`
    Message     string            `json:"message"`
    Fields      DockerFields      `json:"docker"`
}

func init() {
	// Register this adapter
    router.AdapterFactories.Register(NewLogspoutAdapter, "kinesis")
}

func NewLogspoutAdapter(route *router.Route) (router.LogAdapter, error) {
	// Return the kinesis adapter that will receive all the logs
	return &KinesisAdapter{
        route:          route,
        key:            key,
        docker_host:    docker_host,
        use_v0:         use_v0,
    }, nil
}

func (ka *KinesisAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		Print("KinesisAdapter received log message: %s", logstream)
	}
}	
