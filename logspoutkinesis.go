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
    kinesis "github.com/sendgridlabs/go-kinesis"
    batchproducer "github.com/sendgridlabs/go-kinesis/batchproducer"
)

type KinesisAdapter struct {
    route       	*router.Route
    batch_client	*kinesis.Kinesis
    batch_producer 	batchproducer.Producer
    streamName		string
    docker_host 	string
    use_v0    		bool
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
	streamName := route.Address
	log.Printf("Using kinesis stream: %s\n", streamName)
	
	// set env variables AWS_ACCESS_KEY and AWS_SECRET_KEY AWS_REGION_NAME
	auth, err := kinesis.NewAuthFromEnv()
	if err != nil {
		fmt.Printf("Unable to retrieve authentication credentials from the environment: %v", err)
		os.Exit(1)
	}

	aws_region := kinesis.NewRegionFromEnv()
	log.Printf("Using kinesis region: %s\n", aws_region)
	batch_client := kinesis.New(auth, aws_region)

	// Batch config
	batchproducer_config := batchproducer.Config{
	    AddBlocksWhenBufferFull: false,
	    BufferSize:              10000,
	    FlushInterval:           1 * time.Second,
	    BatchSize:               10,
	    MaxAttemptsPerRecord:    10,
	    StatInterval:            1 * time.Second,
	    Logger:                  log.New(os.Stderr, "", log.LstdFlags),
	}

	// Create a batchproducer
	batch_producer, err := batchproducer.New(batch_client, streamName, batchproducer_config)
	if err != nil {
		fmt.Printf("Unable to retrieve create batchproducer: %v", err)
		os.Exit(1)
	}

	// Host of the docker instance
	docker_host := getopt("LK_DOCKER_HOST", "unknown-docker-host")

	// Whether to use the v0 logtstash layout or v1
    use_v0 := route.Options["use_v0_layout"] != ""
    if !use_v0 {
        use_v0 = getopt("LK_USE_V0_LAYOUT", "") != ""
    }

	// Return the kinesis adapter that will receive all the logs
	return &KinesisAdapter{
        route:          route,
        batch_producer: batch_producer,
        streamName: 	streamName,
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
	// Start the producer
	err := ka.batch_producer.Start()
	if err != nil {
		fmt.Printf("Unable to start batchproducer: %v", err)
		os.Exit(1)
	}

	// Register stop
	defer ka.batch_producer.Stop()

	// Handle log messages
	mute := false
	for m := range logstream {
		msg := createLogstashMessage(m, ka.docker_host, ka.use_v0)

		// Create json from log message
        log_json, err := json.Marshal(msg)
        if err != nil {
        	if !mute {
                log.Println("logspoutkinesis: error on json.Marshal (muting until restored): %v\n", err)
                mute = true
            }
            continue
        }

        // Send log message to kinesis
        err := ka.batch_producer.Add(log_json, ka.docker_host)
        if err != nil {
        	if !mute {
                log.Println("logspoutkinesis: error on batchproducer.Stop (muting until restored): %v\n", err)
                mute = true
            }
            continue
        }
 		
		// Unmute
        mute = false
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
