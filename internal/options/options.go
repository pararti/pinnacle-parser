package options

import (
	"errors"
	"os"

	"github.com/go-yaml/yaml"

	"github.com/pararti/pinnacle-parser/pkg/defaults"
)

const site = "https://www.pinnacle.com"

type Options struct {
	CookieDir       string `yaml:"cookieDir,omitempty"`
	Site            string `yaml:"site,omitempty"`
	UserAgent       string `yaml:"userAgent,omitempty"`
	LogPath         string `yaml:"logPath,omitempty"`
	TestMode        bool   `yaml:"testMode,omitempty"`
	KafkaAddress    string `yaml:"kafkaAddress,omitempty"`
	KafkaPort       string `yaml:"kafkaPort,omitempty"`
	KafkaTopic      string `yaml:"kafkaTopic,omitempty"`
	Login           string `yaml:"login,omitempty"`
	Password        string `yaml:"password,omitempty"`
	DbConnection    string `yaml:"dbConnection,omitempty"`
	ProducerSentry  string `yaml:"producerSentry,omitempty"`
	ConsumerSentry  string `yaml:"consumerSentry,omitempty"`
	RemoteChromeURL string `yaml:"remoteChromeURL,omitempty"`
}

func NewOptions() (*Options, error) {
	o := Options{}
	o.fillDefaultValues()

	yamlData, err := os.ReadFile("../config/settings.yaml")
	if err != nil {
		return nil, errors.New("Не удалось загрузить файл конфигурации " + err.Error())
	} else {
		err = yaml.Unmarshal(yamlData, &o)
		if err != nil {
			return nil, errors.New("Не удалось выгрузить файл конфигурации в структуру " + err.Error())
		}
	}

	return &o, nil
}

func (o *Options) fillDefaultValues() {
	o.UserAgent = defaults.UserAgent
	o.Site = site
	o.KafkaAddress = "localhost"
	o.KafkaPort = "9092"
}
