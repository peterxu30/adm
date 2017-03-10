package main

import (
	"fmt"
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

const (
	CONFIG_FILE = "params.yml"
)

type AdmConfig struct {
	SourceUrl string `yaml:"source_url"`
	WorkerSize int `yaml:"worker_size"`
	OpenIO int `yaml:"open_io"`
	UuidDest string `yaml:"uuid_dest"`
	MetadataDest string `yaml:"metadata_dest"`
	TimeseriesDest string `yaml:"timeseries_dest"`
	ReadMode ReadMode `yaml:"read_mode"`
	WriteMode WriteMode `yaml:"write_mode"`
	ChunkSize int64 `yaml:"chunk_size"`
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
	body := []byte("source_url:\nworker_size:\nopen_io:\nuuid_dest:\nmetadata_dest:\ntimeseries_dest:\nread_mode:\nwrite_mode:\nchunk_size:")
	err := ioutil.WriteFile(CONFIG_FILE, body, 0644)
	return err
}
