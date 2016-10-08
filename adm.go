package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
)

const (
	Url                       = "http://castle.cs.berkeley.edu:8079/api/query"
	UuidDestination           = "uuids.txt"
	MetadataDestination       = "metadata.txt"
	TimeseriesDataDestination = "timeseriesdata.txt"
	ChunkSize                 = 2000 //amount of UUIDs each go routine processes
)

type Reader interface { //potentially unnecessary. no point in storing all of this information in memory is there?
	ReadAllUuids() //this is necessary
	ReadAllMetadata()
	ReadAllTimeseriesData()
}

type Writer interface { //allows writing to file or to endpoint
	WriteAllUuids(dest string)    //mainly for logging purposes
	WriteAllMetadata(dest string) //dest can be a url or a file name
	WriteSomeMetadata(dest string, start int, end int)
	WriteAllTimeseriesData(dest string)
	WriteSomeTimeseriesData(dest string, start int, end int)
}

type Metadata struct {
	Path       string
	Uuid       string `json:"uuid"`
	Properties interface{}
	Metadata   interface{}
}

type TimeseriesData struct {
	Uuid     string
	Readings interface{} //array of string arrays
}

type CombinedData struct { //final form of data to send out
	Path       string
	Uuid       string `json:"uuid"`
	Properties interface{}
	Metadata   interface{}
	Readings   interface{}
}

type UuidTuple struct {
	Uuid           string //for logging
	Metadata       []byte
	TimeseriesData []byte
}

type DataCollection struct {
	Log             *Logger
	Url             string
	Uuids           []string
	Metadatas       [][]Metadata       //not used
	TimeseriesDatas [][]TimeseriesData //not used
	DataChan        chan UuidTuple
}

func NewDataCollection(url string) *DataCollection {
	return &DataCollection{
		Log:      newLogger(),
		Url:      url,
		DataChan: make(chan UuidTuple),
	}
}

func (collection *DataCollection) ReadAllUuids() {
	body := makeQuery(collection.Url, "select distinct uuid")
	json.Unmarshal(body, &(collection.Uuids))
}

/* Adds all UUIDs from collection.Uuids to the Bolt log.
 * Uses go routines
 */
func (collection *DataCollection) AddAllUuidsToLog() {
	if collection.Log.getLogMetadata("read_uuids") == WRITE_COMPLETE {
		fmt.Println("UUID Log write complete")
		return
	}
	length := len(collection.Uuids)
	numRoutinesFloat := float64(length) / float64(ChunkSize)
	numRoutines := length / ChunkSize
	if numRoutinesFloat > float64(numRoutines) {
		numRoutines++
	}
	start := 0
	end := ChunkSize
	if numRoutines == 0 {
		numRoutines = 1
		end = length
	}

	var wg sync.WaitGroup
	wg.Add(numRoutines)
	for numRoutines > 0 {
		go collection.AddUuidsToLog(start, end, &wg)
		start = end
		end += 2000
		if end > length {
			end = length
		}
		numRoutines--
	}

	wg.Wait()
	fmt.Println("Read uuids to log complete")
	collection.Log.updateLogMetadata("read_uuids", WRITE_COMPLETE)
}

/* Called as a go routine in AddAllUuidsToLog */
func (collection *DataCollection) AddUuidsToLog(start int, end int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := start; i < end; i++ {
		uuid := collection.Uuids[i]
		collection.Log.updateUuidStatus(uuid, UNSTARTED)
	}
	fmt.Println("Uuids ", start, " to ", end, " added to log")
}

/* Writes metadata to memory. No real purpose */
func (collection *DataCollection) ReadAllMetadata() { //potentially unnecessary
	collection.Metadatas = make([][]Metadata, len(collection.Uuids))
	for i, uuid := range collection.Uuids {
		query := "select * where uuid='" + uuid + "'"
		body := makeQuery(collection.Url, query)
		json.Unmarshal(body, &(collection.Metadatas[i]))
	}
}

/* Writes timeseriesdata to memory. No real purpose */
func (collection *DataCollection) ReadAllTimeseriesData() { //potentially unnecessary
	collection.TimeseriesDatas = make([][]TimeseriesData, len(collection.Uuids))
	for i, uuid := range collection.Uuids {
		query := "select data before now as ns where uuid='" + uuid + "'"
		body := makeQuery(collection.Url, query)
		json.Unmarshal(body, &(collection.TimeseriesDatas[i]))
	}
}

/* Writes all UUIDs to the UuidDestination file.
 * This function is not logged as it is inexpensive. Can be made crash proof if need be.
 * Maybe remove the arguments and add the information as "instance" attributes?
 */
func (collection *DataCollection) WriteAllUuids(dest string) {
	uuidBytes, err := json.Marshal(collection.Uuids)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(dest, uuidBytes, 0644)
	if err != nil {
		panic(err)
	}
}

/* Main method to query and write the metadata and timeseriesdata for all UUIDs to files.
 * Calls WriteDataBlock in go routines to query metadata and timeseriesdata and write them to a channel.
 * Calls WriteFromChannel in a go routine to read from the channel and write to file.
 * More details on the specific operations of these two functions can be found below.
 * SIMD parallelization. Go routines are called with a set block of UUIDs to process.
 */
func (collection *DataCollection) WriteAllDataBlocks(metaDest string, timeseriesDest string) {
	defer close(collection.DataChan) //close the channel once writes are complete

	length := len(collection.Uuids)
	numRoutinesFloat := float64(length) / float64(ChunkSize)
	numRoutines := length / ChunkSize
	if numRoutinesFloat > float64(numRoutines) {
		numRoutines++
	}
	start := 0
	end := ChunkSize
	if numRoutines == 0 {
		numRoutines = 1
		end = length
	}

	var wg sync.WaitGroup
	wg.Add(numRoutines)
	go collection.WriteFromChannel(metaDest, timeseriesDest, collection.DataChan)
	for numRoutines > 0 {
		go collection.WriteDataBlock(start, end, &wg) //writes to channel
		start = end
		end += ChunkSize
		if end > length {
			end = length
		}
		numRoutines--
	}
	//close channel here
	wg.Wait()
}

/* Makes queries for all UUIDs within the range of start to end.
 * Wraps UUID, metadata, and timeseriesdata in a UuidTuple and passes it into the channel.
 * Checks if each UUID has been written previously.
 * Should only be called as a go routine.
 */
func (collection *DataCollection) WriteDataBlock(start int, end int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := start; i < end; i++ {
		uuid := collection.Uuids[i]

		status := collection.Log.getUuidStatus(uuid)

		if status == WRITE_COMPLETE {
			continue
		}

		//read into channel
		mQuery := "select * where uuid='" + uuid + "'"
		mBody := makeQuery(collection.Url, mQuery)

		tQuery := "select data before now as ns where uuid='" + uuid + "'"
		tBody := makeQuery(collection.Url, tQuery)

		tuple := UuidTuple{
			Uuid:           uuid,
			Metadata:       mBody,
			TimeseriesData: tBody,
		}

		collection.DataChan <- tuple
	}
	fmt.Println("Block ", start, " - ", end, " read complete")
}

/* Should only be called once in a single go routine
 * dest1 - metadata
 * dest2 - timeseries
 * WriteFromChannel reads from the input channel and writes the metadata and timeseriesdata within the UuidTuple to file.
 * Does not check if UUID was already written. This is responsibility of the WriteDataBlock method.
 */
func (collection *DataCollection) WriteFromChannel(dest1 string, dest2 string, chnnl chan UuidTuple) {
	writeStatus := collection.Log.getLogMetadata("write_status")

	if writeStatus == WRITE_COMPLETE {
		fmt.Println("Files ", dest1, ", ", dest2, " already written.")
		return
	}

	// In order to ensure correct JSON format
	if writeStatus == UNSTARTED {
		err := ioutil.WriteFile(dest1, []byte("["), 0666)
		if err != nil {
			panic(err)
		}

		err = ioutil.WriteFile(dest2, []byte("["), 0666)
		if err != nil {
			panic(err)
		}
		collection.Log.updateLogMetadata("write_status", WRITE_START)
	}

	f1, err := os.OpenFile(dest1, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}

	f2, err := os.OpenFile(dest2, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}

	first := true
	for data := range chnnl {
		uuid := data.Uuid
		collection.Log.updateUuidStatus(uuid, WRITE_START)

		metadata := data.Metadata
		timeseriesdata := data.TimeseriesData

		if !first {
			f1.Write([]byte(","))
			f2.Write([]byte(","))
		}
		f1.Write(metadata)
		f2.Write(timeseriesdata)
		fmt.Println("Write Complete: ", uuid)
		collection.Log.updateUuidStatus(uuid, WRITE_COMPLETE)
	}

	//TODO: Potential to write too many "]" if crashes here.
	f1.Write([]byte("]")) //fix this for WAL
	f2.Write([]byte("]")) //fix this for WAL
	collection.Log.updateLogMetadata("write_status", WRITE_COMPLETE)
}

/* General purpose function to make an HTTP POST request to the specified url
 * with the specified queryString.
 * Return value is of type []byte. It is up to the calling function to convert
 * []byte into the appropriate type.
 */
func makeQuery(url string, queryString string) []byte {
	query := []byte(queryString)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(query))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return body
}

/* For test purposes. Not used. */
func readMetadataFile(index int) (mdata [][]Metadata) {
	file := "metadata_example.txt"
	dat, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	json.Unmarshal(dat, &mdata)
	current_mdata := mdata[index][0]
	fmt.Println(current_mdata.Path)
	fmt.Println(current_mdata.Uuid)
	fmt.Println(current_mdata.Properties)
	fmt.Println(current_mdata.Metadata)
	return mdata
}

func main() {
	collection := NewDataCollection(Url)
	collection.ReadAllUuids()
	collection.AddAllUuidsToLog()
	collection.WriteAllUuids(UuidDestination)
	collection.WriteAllDataBlocks(MetadataDestination, TimeseriesDataDestination)
	fmt.Println("Number of UUIDs: ", len(collection.Uuids))
	fmt.Println("Migration complete.")
	// fmt.Println(collection.Log.getUuidStatus(collection.Uuids[0]), UNSTARTED)
}
