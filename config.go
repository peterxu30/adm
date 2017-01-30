package main

import (
	"fmt"
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

const (
	CONFIG_FILE = "config.yml"
)

type AdmConfig struct {
	SourceUrl string `yaml:"sourceUrl"`
	WorkerSize int `yaml:"workerSize"`
	OpenIO int `yaml:"openIO"`
	UuidDest string `yaml:"uuidDest"`
	MetadataDest string `yaml:"metadataDest"`
	TimeseriesDest string `yaml:"timeseriesDest"`
	ReadMode string `yaml:"readMode"`
	WriteMode string `yaml:"writeMode"`
	ChunkSize int `yaml:"chunkSize"`
	ChannelBufferSize int `yaml:"channelBufferSize"`
}

func newAdmConfig() (*AdmConfig, error) {
	configData, err := readConfigFile()
	if err != nil {
		return nil, err
	}

	admConfig := AdmConfig{}
	err = yaml.Unmarshal(configData, &admConfig)
	fmt.Println(admConfig)
	if err != nil {
		return nil, err
	}

	return &admConfig, nil
}

func readConfigFile() ([]byte, error) {
	configData, err := ioutil.ReadFile(CONFIG_FILE)
	fmt.Println(string(configData))
	if err != nil {
		return nil, err
	}
	return configData, nil
}
