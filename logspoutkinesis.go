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
    stream      string
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
	// The kinesis stream where the logs should be sent to
	stream := route.Options["stream"]
    if stream == "" {
        stream = getopt("LK_AWS_KINESIS_STREAM", "logspout")
    }

	// Host of the docker instance
	docker_host := getopt("LK_DOCKER_HOST", "")

	// Whether to use the v0 logtstash layout or v1
    use_v0 := route.Options["use_v0_layout"] != ""
    if !use_v0 {
        use_v0 = getopt("LK_USE_V0_LAYOUT", "") != ""
    }

	// Return the kinesis adapter that will receive all the logs
	return &KinesisAdapter{
        route:          route,
        stream:         stream,
        docker_host:    docker_host,
        use_v0:         use_v0,
    }, nil
}

func getopt(name, dfault string) string {
    value := os.Getenv(name)
    if value == "" {
        value = dfault
    }
    return value
}

func (ka *KinesisAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		fmt.Printf("KinesisAdapter received log message: %s", m)
		msg := createLogstashMessage(m, ka.docker_host, ka.use_v0)
        js, err := json.Marshal(msg)
        if err != nil {
        	fmt.Printf("KinesisAdapter received log message: %s", err)
            log.Println("logspoutkinesis: error on json.Marshal (muting until restored):", err)
            continue
        }
		fmt.Printf("KinesisAdapter send json message: %s", js)
	}
}	

func splitImage(image string) (string, string) {
    n := strings.Index(image, ":")
    if n > -1 {
        return image[0:n], image[n+1:]
    }
    return image, ""
}

func createLogstashMessage(m *router.Message, docker_host string, use_v0 bool) interface{} {
    image_name, image_tag := splitImage(m.Container.Config.Image)
    cid := m.Container.ID[0:12]
    name := m.Container.Name[1:]
    timestamp := m.Time.Format(time.RFC3339Nano)

    if use_v0 {
        return LogstashMessageV0{
            Message:    m.Data,
            Timestamp:  timestamp,
            Sourcehost: m.Container.Config.Hostname,
            Fields:     LogstashFields{
                Docker: DockerFields{
                    CID:        cid,
                    Name:       name,
                    Image:      image_name,
                    ImageTag:   image_tag,
                    Source:     m.Source,
                    DockerHost: docker_host,
                },
            },
        }
    }

    return LogstashMessageV1{
        Message:    m.Data,
        Timestamp:  timestamp,
        Sourcehost: m.Container.Config.Hostname,
        Fields:     DockerFields{
            CID:        cid,
            Name:       name,
            Image:      image_name,
            ImageTag:   image_tag,
            Source:     m.Source,
            DockerHost: docker_host,
        },
    }
}
