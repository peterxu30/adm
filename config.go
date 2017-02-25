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
	ReadMode ReadMode `yaml:"readMode"`
	WriteMode WriteMode `yaml:"writeMode"`
	ChunkSize int64 `yaml:"chunkSize"`
}

func newAdmConfig() (*AdmConfig, error) {
	configData, err := readConfigFile()
	if err != nil {
		return nil, err
	}

	admConfig := AdmConfig{}
	err = yaml.Unmarshal(configData, &admConfig)
	fmt.Println(admConfig)
	fmt.Println(admConfig.ReadMode, RM_GILES, RM_FILE)
	fmt.Println(admConfig.WriteMode, WM_GILES, WM_FILE)
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

func createConfigFile() error {
	body := []byte("sourceUrl:\nworkerSize:\nopenIO:\nuuidDest:\nmetadataDest:\ntimeseriesDest:\nreadMode:\nwriteMode:\nchunkSize:")
	err := ioutil.WriteFile(CONFIG_FILE, body, 0644)
	return err
}
