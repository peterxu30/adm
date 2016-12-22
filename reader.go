/*
The Reader interface is designed so that every method is designed for sequential operation.
It is up to the calling function to manage any parallelization.
Reader-implemented objects are designed to read in raw bytes and leave any unmarshalling to Writer-implemented classes.
Reader methods should be idempotent.
*/

package main

type MetadataTuple struct {
    uuid           string //for logging
    data       []byte
}

type TimeseriesTuple struct {
	slot *TimeSlot
	data []byte
}

type Reader interface {
    readUuids(src string) []string //relatively small size. can be accomplished without use of channels.
    readMetadata(src string, uuids []string, dataChan chan *MetadataTuple)
    readTimeseriesData(src string, slots []*TimeSlot, dataChan chan *TimeseriesTuple)
}
