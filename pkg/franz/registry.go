package franz

import (
	"net/http"
	"sync"
	"time"

	schemaregistry "github.com/landoop/schema-registry"
	"github.com/sirupsen/logrus"
)

const timeout = 5 * time.Second

type Schema schemaregistry.Schema

type Registry interface {
	Subjects() ([]string, error)
	SchemaByID(uint32) (string, error)
	SchemaBySubject(string) (Schema, error)
}

type nilRegistry struct{}

func (n nilRegistry) Subjects() ([]string, error)            { return nil, ErrNoRegistry }
func (n nilRegistry) SchemaByID(uint32) (string, error)      { return "", ErrNoRegistry }
func (n nilRegistry) SchemaBySubject(string) (Schema, error) { return Schema{}, ErrNoRegistry }

type defaultRegistry struct {
	client *schemaregistry.Client
	log    logrus.FieldLogger

	mutex sync.Mutex
	cache map[uint32]string
}

func newRegistry(config Config, log logrus.FieldLogger) (*defaultRegistry, error) {
	client := http.Client{Timeout: timeout}
	if config.TLSConfig != nil {
		config, err := config.TLSConfig.loadTLSConfig()
		if err != nil {
			return nil, err
		}

		client.Transport = &http.Transport{
			TLSClientConfig: config,
		}
	}

	c, err := schemaregistry.NewClient(config.SchemaRegistry, schemaregistry.UsingClient(&client))
	if err != nil {
		return nil, err
	}

	return &defaultRegistry{
		client: c,
		cache:  map[uint32]string{},
		log:    log,
	}, nil
}

func (r *defaultRegistry) Subjects() ([]string, error) {
	return r.client.Subjects()
}

func (r *defaultRegistry) SchemaByID(id uint32) (string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, ok := r.cache[id]; !ok {
		schema, err := r.client.GetSchemaByID(int(id))
		if err != nil {
			return "", err
		}

		r.log.Infof("retrieved schema with ID %d", id)

		r.cache[id] = schema
	}

	return r.cache[id], nil
}

func (r *defaultRegistry) SchemaBySubject(subject string) (Schema, error) {
	schema, err := r.client.GetLatestSchema(subject)
	if err != nil {
		return Schema{}, err
	}

	return Schema(schema), nil
}
