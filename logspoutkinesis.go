// Based on:
// - https://github.com/rtoma/logspout-redis-logstash/blob/master/redis.go

package logspoutkinesis

import (
    "fmt"
    "strings"
    "strconv"
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
    Name        string              `json:"name"`
    CID         string              `json:"cid"`
    Image       string              `json:"image"`
    ImageTag    string              `json:"image_tag,omitempty"`
    Source      string              `json:"source"`
    DockerHost  string              `json:"docker_host,omitempty"`
    Labels      map[string]string   `json:"labels,omitempty"`
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
	// kinesis client
    batch_client := getKinesis(route)

    // The kinesis stream where the logs should be sent to
    streamName := route.Address
    fmt.Printf("# KINESIS Adapter - Using stream: %s\n", streamName)
    
    // Batch config
    batchproducer_config := getKinesisConfig(route)
    fmt.Printf("# KINESIS Adapter - Batch config: %v\n", streamName)
    
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

func getKinesis(route *router.Route) *kinesis.Kinesis {
    // set env variables AWS_ACCESS_KEY and AWS_SECRET_KEY AWS_REGION_NAME
    auth, err := kinesis.NewAuthFromEnv()
    if err != nil {
        fmt.Printf("Unable to retrieve authentication credentials from the environment: %v", err)
        os.Exit(1)
    }

    // AWS region
    aws_region := kinesis.NewRegionFromEnv()
    fmt.Printf("# KINESIS Adapter - Using region: %s\n", aws_region)

    return kinesis.New(auth, aws_region)
}

func getKinesisConfig(route *router.Route) batchproducer.Config {
    AddBlocksWhenBufferFull := false
    AddBlocksWhenBufferFull_string, ok := route.Options["add_blocks_when_buffer_full"];
    if ok && AddBlocksWhenBufferFull_string != "" {
        if b, err := strconv.ParseBool(AddBlocksWhenBufferFull_string); err == nil {
            AddBlocksWhenBufferFull = b
        }
    }

    BufferSize := 10000
    BufferSize_string, ok := route.Options["buffer_size"];
    if ok && BufferSize_string != "" {
        if i, err := strconv.ParseInt(AddBlocksWhenBufferFull_string, 10, 0); err == nil {
            BufferSize = int(i)
        }
    }

    FlushInterval := 1 * time.Second
    FlushInterval_string, ok := route.Options["flush_interval"];
    if ok && FlushInterval_string != "" {
        if i, err := strconv.ParseInt(FlushInterval_string, 10, 0); err == nil {
            FlushInterval = time.Duration(i) * time.Second
        }
    }

    BatchSize := 10
    BatchSize_string, ok := route.Options["batch_size"];
    if ok && BatchSize_string != "" {
        if i, err := strconv.ParseInt(BatchSize_string, 10, 0); err == nil {
            BatchSize = int(i)
        }
    }

    MaxAttemptsPerRecord := 10
    MaxAttemptsPerRecord_string, ok := route.Options["max_attempts_per_record"];
    if ok && MaxAttemptsPerRecord_string != "" {
        if i, err := strconv.ParseInt(MaxAttemptsPerRecord_string, 10, 0); err == nil {
            MaxAttemptsPerRecord = int(i)
        }
    }

    StatInterval := 1 * time.Second
    StatInterval_string, ok := route.Options["start_interval"];
    if ok && StatInterval_string != "" {
        if i, err := strconv.ParseInt(StatInterval_string, 10, 0); err == nil {
            StatInterval = time.Duration(i) * time.Second
        }
    }

    return batchproducer.Config{
        AddBlocksWhenBufferFull: AddBlocksWhenBufferFull,
        BufferSize:              BufferSize,
        FlushInterval:           FlushInterval,
        BatchSize:               BatchSize,
        MaxAttemptsPerRecord:    MaxAttemptsPerRecord,
        StatInterval:            StatInterval,
        Logger:                  log.New(os.Stderr, "kinesis: ", log.LstdFlags),
    }
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
        err = ka.batch_producer.Add(log_json, ka.docker_host)
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
    labels := m.Container.Config.Labels
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
                    Labels: labels,
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
            Labels: labels,
        },
    }
}
