/*
The Reader interface is designed so that every method is designed for sequential operation.
It is up to the calling function to manage any parallelization.
Reader-implemented objects are designed to read in raw bytes and leave any unmarshalling to Writer-implemented classes.
*/

package main

type DataTuple struct {
    uuid           string //for logging
    data       []byte
}



type Reader interface {
    readUuids() []string //relatively small size. can be accomplished without use of channels.
    readMetadata(uuids []string, dataChan chan *DataTuple)
    readTimeseriesData(slots []*TimeSlot, dataChan chan *DataTuple)
}
