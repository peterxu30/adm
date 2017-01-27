/*
The Writer interface is designed so that every method is designed for sequential operation.
It is up to the calling function to manage any parallelization.
Writer-implemented objects read in raw bytes and do whatever unmarshalling is necessary. 
Writer methods should be idempotent.
Updating logMetadata should be handled by the caller.
*/

package main

type Metadata struct {
    path       string
    uuid       string `json:"uuid"`
    properties interface{}
    metadata   interface{}
}

type TimeseriesData struct {
    uuid     string
    readings interface{} //array of string arrays
}

type Writer interface { //allows writing to file or to endpoint
	writeUuids(dest string, uuids []string) *ProcessError
	writeMetadata(dest string, dataChan chan *MetadataTuple) *ProcessError
	writeTimeseriesData(dest string, dataChan chan *TimeseriesTuple) *ProcessError
}
