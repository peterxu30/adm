package main

const (
    MetadataDestination = "metadata.txt"
    UuidDestination = "uuids.txt"
    FileSize = 10000000 //amount of records in each timeseries file
)

type FileWriter struct {

}

func newFileWriter() *FileWriter {
	return &FileWriter{

	}
}

func (w *FileWriter) writeUuids(uuids []string) {}

func (w *FileWriter) writeMetadata(dataChan chan *DataTuple) {}

func (w *FileWriter) writeTimeseriesData(dataChan chan *DataTuple) {}
