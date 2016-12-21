package main

type Metadata struct {
    path       string
    uuid       string `json:"uuid"`
    properties interface{}
    metadata   interface{}
}

type MetadataTuple struct {
    uuid           string //for logging
    metadata       []byte
}

type Reader interface {
    readUuids() []string
    readMetadata(uuids []string, metadataChan chan MetadataTuple)
    readTimeseriesData()
}
