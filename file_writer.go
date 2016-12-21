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

func (w *FileWriter) writeUuids() {}

func (w *FileWriter) writeMetadata(metadataChan chan MetadataTuple) {}

func (w *FileWriter) writeTimeseriesData() {}