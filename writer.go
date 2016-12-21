package main

type TimeseriesData struct {
    uuid     string
    readings interface{} //array of string arrays
}

type Writer interface { //allows writing to file or to endpoint
	writeUuids()
	writeMetadata(metadataChan chan MetadataTuple)
	writeTimeseriesData()
    // WriteAllUuids(dest string)    //mainly for logging purposes
    // WriteAllMetadata(dest string) //dest can be a url or a file name
    // WriteSomeMetadata(dest string, start int, end int)
    // WriteAllTimeseriesData(dest string)
    // WriteSomeTimeseriesData(dest string, start int, end int)
}
