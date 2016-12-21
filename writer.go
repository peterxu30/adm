/*
The Writer interface is designed so that every method is designed for sequential operation.
It is up to the calling function to manage any parallelization.
Writer-implemented objects read in raw bytes and do whatever unmarshalling is necessary. 
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
	writeUuids()
	writeMetadata(dataChan chan DataTuple)
	writeTimeseriesData(dataChan chan DataTuple)
}
