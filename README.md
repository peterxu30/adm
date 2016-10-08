# adm
Archived Data Migrator
For Software Defined Buildings

##Program Flow
adm uses several Go routines to simultaneously query the specified URL and writes the output to a Go channel.
A single go routine reads from this channel and writes the contents to files.

##Constants
While a user interface is not yet available, there are a few parameters that are modifiable.
These are the constants defined in adm.go.

1. `Url`: The URL to query.
2. `UuidDestination`: The file to write UUIDs to.
3. `MetadataDestination`: The file to write metadata to.
4. `TimeseriesDataDestination`: The file to write timeseries data to.
5. `ChunkSize`: The number of UUIDs each go routine will process. This determines the number of go routines created.
