# adm
Archived Data Migrator
For Software Defined Buildings

## Program Flow
adm works in a parallelized manner with go routines reading from the source and writing to channels from which other go routines read and write to specified destinations.

adm can be abstracted to three main components. adm itself as well as reader and writer APIs that conform to the Reader and Writer interface.
1. adm - Responsible for parallelizing and assigning tasks to readers and writers as well as logging progress.
2. Readers - Responsible for reading data from a specified source. This can be through HTTP calls or reading local files.
3. Writers - Responsible for writing data to a specified destination. Destinations can be local files or a network endpoint.

## How to Run
1. Clone the repo.
2. Install dependencies: `go get`
3. Build adm: `go build`
4. Run adm: `./adm`

## Configurations
The `params.yml` file contains modifiable settings. Settings are listed below:
1. source_url: Source url to query for data. (TODO: Allow for multiple sources for metadata, timeseries data, etc.)
2. worker_size: Loose bound on maximum number of running go routines allowed.
3. open_io: Bound on maximum number of open IOs allowed.
4. metadata_dest: Destination of metadata. Can be a text file or a URL.
5. timeseries_dest: Destination of timeseries data. Can be a text file or a URL.
6. read_mode: Specified read mode (See Read Mode)                                         
7. write_mode: Specified write mode (See Write Mode)                          
8. chunk_size: Rough estimate of number of timeseries tuples to process per go routine.                           

## Read Mode
adm supports reading data from various sources. Modes are integers corresponding to an implementation of the Reader interface. As of now, only one read mode is implemented.
```
type Reader interface {
    readUuids(src string) ([]string, *ProcessError)
    readWindows(src string, uuids []string) ([]*Window, *ProcessError)
    readMetadata(src string, uuids []string, dataChan chan *MetadataTuple) *ProcessError
    readTimeseriesData(src string, slots []*TimeSlot, dataChan chan *TimeseriesTuple) *ProcessError
}
```
1. 1 (Giles Mode) - Read from Giles endpoint.

## Write Mode
adm supports writing data to various destinations. Modes are integers corresponding to an implementation of the Writer interface. As of now, only one write mode is implemented.
```
type Writer interface {
	writeUuids(dest string, uuids []string) *ProcessError
	writeMetadata(dest string, dataChan chan *MetadataTuple) *ProcessError
	writeTimeseriesData(dest string, dataChan chan *TimeseriesTuple) *ProcessError
}
```
1. 2 (File Mode) - Write to local files.

## Required Libraries
1. [Bolt](https://github.com/boltdb/bolt)
2. [go-yaml](gopkg.in/yaml.v2)
