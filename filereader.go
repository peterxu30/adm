package main

import (
	"encoding/json"
	"io/ioutil"
)
// To read from files
// Assumes all necessary files already exist
type FileReader struct {
    Url string
    UuidFile string
    MetadataFile string
    TimeseriesFile string
    Uuids []string
    Metadatas [][]Metadata
    TimeseriesDatas [][]TimeseriesData
}

func NewFileReader(url string, uuidFile string, metadataFile string, timeseriesFile string) *FileReader {
	return &FileReader {
		Url: url,
		UuidFile: uuidFile,
		MetadataFile: metadataFile,
		TimeseriesFile: timeseriesFile,
	}
}

func (reader *FileReader) ReadAllUuids() {
	data, err := ioutil.ReadFile(reader.UuidFile)
	if err != nil {
        panic(err)
    }
    json.Unmarshal(data, &(reader.Uuids))
}

func (reader *FileReader) ReadAllMetadata() {
	reader.Metadatas = make([][]Metadata, len(reader.Uuids))
	data, err := ioutil.ReadFile(reader.MetadataFile)
    if err != nil {
        panic(err)
    }
    json.Unmarshal(data, &(reader.Metadatas))
}

func (reader *FileReader) ReadAllTimeseriesData() { //work in progress
	reader.TimeseriesDatas = make([][]TimeseriesData, len(reader.Uuids))
	data, err := ioutil.ReadFile(reader.MetadataFile)
    if err != nil {
        panic(err)
    }
    json.Unmarshal(data, &(reader.Metadatas))
}