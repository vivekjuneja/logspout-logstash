package logstash

import (
	"encoding/json"
	"errors"
	"github.com/gliderlabs/logspout/router"
	"log"
	"net"
	"strings"
	"github.com/fsouza/go-dockerclient"
	"strconv"
)

var counter = 0

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
	containerTags map[string][]string
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
		containerTags: make(map[string][]string),
	}, nil
}


	// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}
	var tags = []string{}
	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tags = strings.Split(strings.TrimPrefix(e, "LOGSTASH_TAGS="), ",")
			break
		}
	}
	a.containerTags[c.ID] = tags
	return tags
}


// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}

		tags := GetContainerTags(m.Container, a)

		var js []byte
		var data map[string]interface{}

		// Parse JSON-encoded m.Data
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			// The message is not in JSON, make a new JSON message.
			msg := LogstashMessage{
				Message: m.Data,
				Docker:  dockerInfo,
				Stream:  m.Source,
				Tags:    tags,
				Name:     m.Container.Name,
				ID:       m.Container.ID,
				Image:    m.Container.Config.Image,
				Hostname: m.Container.Config.Hostname,
				LogId:    "UNKNOWN",
			}

			for _, kv := range m.Container.Config.Env {
				kvp := strings.SplitN(kv, "=", 2)

				if kvp[0] == "LOGID" {
					msg.LogId = kvp[1]
					counter = counter + 1
					log.Println("Value : ", counter)
					msg.LogSequenceId = strconv.Itoa(counter)

				} else if kvp[0] == "TYPE" {
					msg.Type = kvp[1]
				} else if kvp[0] == "MESOS_TASK_ID" {
					msg.TaskId = kvp[1]
				}

			}	

			if js, err = json.Marshal(msg); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		} else {

			msg := LogstashMessage{
				Message: m.Data,
				Docker:  dockerInfo,
				Stream:  m.Source,
				Tags:    tags,
				Name:     m.Container.Name,
				ID:       m.Container.ID,
				Image:    m.Container.Config.Image,
				Hostname: m.Container.Config.Hostname,
				LogId:    "UNKNOWN",
			}

			for _, kv := range m.Container.Config.Env {
				kvp := strings.SplitN(kv, "=", 2)

				if kvp[0] == "LOGID" {
					msg.LogId = kvp[1]
					counter = counter + 1
					log.Println("Value : ", counter)
					msg.LogSequenceId = strconv.Itoa(counter)
				} else if kvp[0] == "TYPE" {
					msg.Type = kvp[1]
				} else if kvp[0] == "MESOS_TASK_ID" {
					msg.TaskId = kvp[1]
				}

			}	

			// The message is already in JSON, add the docker specific fields.
			data["docker"] = dockerInfo
			data["tags"] = tags
			data["stream"] = m.Source
			data["docker_name"] = m.Container.Name
			data["docker_id"] = m.Container.ID
			data["docker_hostname"] = m.Container.Config.Hostname
			data["docker_image"] = m.Container.Config.Image
			data["logid"] = msg.LogId
			data["type"] = msg.Type
			data["taskId"] = msg.TaskId

			// Return the JSON encoding
			if js, err = json.Marshal(data); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		}

		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		if _, err := a.conn.Write(js); err != nil {
			// There is no retry option implemented yet
			log.Fatal("logstash: could not write:", err)
		}


		
	}
}


type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message  string `json:"message"`
	Stream  string     `json:"stream"`
	Docker  DockerInfo `json:"docker"`
	Tags    []string   `json:"tags"`
	Name     string `json:"docker_name"`
	ID       string `json:"docker_id"`
	Image    string `json:"docker_image"`
	Hostname string `json:"docker_hostname"`
	LogId    string `json:"logid"`
	LogSequenceId string `json:"sequence"`
	Type     string `json:"type"`
	TaskId   string `json:"taskId"`
}