package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
    "strconv"
	"sync"
)

const (
	Url                       = "http://castle.cs.berkeley.edu:8079/api/query"
	UuidDestination           = "uuids.txt"
	MetadataDestination       = "metadata.txt"
	TimeseriesDataDestination = "timeseriesdata.txt"
	ChunkSize                 = 2000 //amount of UUIDs each go routine processes
    FileSize = 10000000 //amount of records in each timeseries file
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
		query := "select data in (now, now - 30d) as ns where uuid='" + uuid + "'"
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
	wg.Add(numRoutines + 1)
	go collection.WriteFromChannel(metaDest, collection.DataChan)
    go collection.WriteDataBlock(0, length, &wg)
	for numRoutines > 0 {
		// go collection.WriteDataBlock(start, end, &wg) //writes to channel
        go collection.NewWriteDataBlock(start, end, &wg)
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

type WindowData struct {
    Uuid string `json:"uuid"`
    Readings [][]int
}

func (collection *DataCollection) getAllWindowData() []*WindowData {
    fmt.Println("get all window data")
    return collection.getSomeWindowData(0, len(collection.Uuids))
}

func (collection *DataCollection) getSomeWindowData(start int, end int) []*WindowData {
    fmt.Println("some window data: ", start, end)
    windows := make([]*WindowData, end-start)
    for i := start; i < end; i++ {
        // fmt.Println("Get some window data: ", i)
        windows[i-start] = collection.getWindowData(collection.Uuids[i])
    }
    return windows
}

/* Helper method that finds the number of timeseriesdata for a given uuid. Used for
 * determining how many files to write timeseriesdata to.
 */
func (collection *DataCollection) getWindowData(uuid string) *WindowData {
    query := "select window(365d) data in (0, now) where uuid = '" + uuid + "'"
    body := makeQuery(collection.Url, query)
    var windows [1]WindowData
    json.Unmarshal(body, &windows) //check error
    window := windows[0]
    // fmt.Println("WINDOW:", window)
    // fmt.Println("WINDOW", string(body))
    return &window
}

type TimeSlot struct {
    Uuid string
    Timestamp int
    Count int
}

func (window *WindowData) getTimeSlots() []*TimeSlot {
    var slots = make([]*TimeSlot, len(window.Readings))
    count := 0
    // fmt.Println("getTimeSlots window:", window)
    for _, reading := range window.Readings {
        if len(reading) < 0 {
            continue
        }
        // fmt.Println("getTimeSlots reading:", reading)
        var slot TimeSlot = TimeSlot {
            Uuid: window.Uuid,
            Timestamp: reading[0],
            Count: reading[1],
        }
        slots[count] = &slot
        count++
    }
    return slots
}

func (collection *DataCollection) NewWriteDataBlock(start int, end int, wg *sync.WaitGroup) {
    defer wg.Done()
    fmt.Println("Getting some windows")
    windowData := collection.getSomeWindowData(start, end)
    fmt.Println("Got some windows")
    completeUuidsToWrite := make([]string, 0, end-start)
    completeUuidsIndex := 0
    fileCount := 0
    currentSize := 0
    if (len(windowData) == 0) {
        return
    }
    firstWindow := windowData[0]
    // fmt.Println("Got first window", firstWindow)
    // firstTimeSlots := firstWindow.getTimeSlots()
    // if (len(firstTimeSlots == 0))

    // var startTimeSlot *TimeSlot = &TimeSlot {
    //     Uuid: firstWindow.Uuid,
    //     Timestamp: 0,
    //     Count: 0,
    // }
    var startTimeSlot *TimeSlot = nil
    var endTimeSlot *timeSlot = nil
    fmt.Println("About to find a chunk to write")
    var innerWg sync.WaitGroup
    for _, window := range windowData { //each window represents one uuid
        fmt.Println("Current size: " + strconv.Itoa(currentSize))
        startTimeSlot = nil
        endTimeSlot = nil
        var timeSlots []*TimeSlot
        timeSlots = window.getTimeSlots()
        for _, timeSlot := range timeSlots {
            endTimeSlot = timeSlot
            if currentSize >= FileSize { //convert to memory size check later
                innerWg.Add(1)
                fileName := strconv.Itoa(start) + "_" + strconv.Itoa(fileCount)
                //go write timeseries
                fmt.Println("Writing timeseries data for " + startTimeSlot.Uuid + " to " + endTimeSlot.Uuid + " size: " + strconv.Itoa(currentSize))
                collection.WriteSomeTimeseriesData(fileName, startTimeSlot, completeUuidsToWrite, endTimeSlot, &innerWg)
                //go write metadata probably don't need to split up metadata as much as timeseries data.
                completeUuidsToWrite = make([]string, 0, end-start)//clear the array
                completeUuidsIndex = 0
                currentSize = 0
                startTimeSlot = timeSlot
                fileCount++
            }
            currentSize += timeSlot.Count
        }
        // completeUuidsToWrite[completeUuidsIndex] = window.Uuid
        completeUuidsToWrite = append(completeUuidsToWrite, window.Uuid)
        completeUuidsIndex++
    }
    // innerWg.Wait()
}

/* UNUSED */
func (collection *DataCollection) WriteSomeMetadata(dest string, start int, end int) {
    // defer collection.Wg.Done()
    err := ioutil.WriteFile(dest, []byte("["), 0644)
    if err != nil {
        panic(err)
    }
    f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0666)
    if err != nil {
        panic(err)
    }
    for i := start; i < end; i++ {
        uuid := collection.Uuids[i]
        if i > 0 {
            f.Write([]byte(","))
        }
        if i%50 == 0 {
            length := strconv.Itoa(i)
            fmt.Println(length + " ids processed.")
        }
        query := "select * where uuid='" + uuid + "'"
        body := makeQuery(collection.Url, query)
        f.Write(body)
    }
    f.Write([]byte("]"))
    f.Close()
}
 
 /* Used
  * Should only be called as a go routine
  */
func (collection *DataCollection) WriteSomeTimeseriesData(dest string, start *TimeSlot, fullUuids []string, end *TimeSlot, wg *sync.WaitGroup) {
    defer wg.Done()
    fmt.Println("Writing timeseriesdata\n")
    err := ioutil.WriteFile(dest, []byte("["), 0644)
    if err != nil {
        panic(err)
    }
    f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0666)
    if err != nil {
        panic(err)
    }
    //write timeslot start
    if start != nil {
        startQuery := "select data in (" + strconv.Itoa(start.Timestamp) + ", now) as ns where uuid='" + start.Uuid + "'"
        fmt.Println("START QUERY: " + startQuery)
        startBody := makeQuery(collection.Url, startQuery)
        f.Write(startBody)  
    } else {
        fmt.Println("No start query")
    }
      

    for i, uuid := range fullUuids {
        // uuid := collection.Uuids[i]
 
        // if i > 0 {
        f.Write([]byte(","))
        // }
        if i%50 == 0 {
            length := strconv.Itoa(i)
            fmt.Println(length + " ids processed.")
        }
        query := "select data in (0, now) as ns where uuid='" + uuid + "'"
        body := makeQuery(collection.Url, query)
        f.Write(body)
    }

    //write timeslot end
    f.Write([]byte(","))
    endQuery := "select data in (0, " + strconv.Itoa(end.Timestamp) + ") as ns where uuid='" + end.Uuid + "'"
    fmt.Println("END QUERY: " + endQuery)
    endBody := makeQuery(collection.Url, endQuery)
    f.Write(endBody)
    f.Write([]byte("]"))
    f.Close()
    fmt.Println("Wrote " + strconv.Itoa(1 + len(fullUuids)) + " uuids")
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

		// tQuery := "select data before now as ns where uuid='" + uuid + "'"
        // tQuery := "select data in (now, now -30d) as ns where uuid='" + uuid + "'"
		// tBody := makeQuery(collection.Url, tQuery)

		tuple := UuidTuple{
			Uuid:           uuid,
			Metadata:       mBody,
		}

		collection.DataChan <- tuple
	}
	fmt.Println("Block ", start, " - ", end, " read complete")
}

/* NOW ONLY WRITES METADATA
 * Should only be called once in a single go routine
 * dest1 - metadata
 * dest2 - timeseries
 * WriteFromChannel reads from the input channel and writes the metadata and timeseriesdata within the UuidTuple to file.
 * Does not check if UUID was already written. This is responsibility of the WriteDataBlock method.
 */
func (collection *DataCollection) WriteFromChannel(dest1 string, chnnl chan UuidTuple) {
	writeStatus := collection.Log.getLogMetadata("write_status")

	if writeStatus == WRITE_COMPLETE {
		fmt.Println("Files ", dest1, " already written.")
		return
	}

	// In order to ensure correct JSON format
	if writeStatus == UNSTARTED {
		err := ioutil.WriteFile(dest1, []byte("["), 0666)
		if err != nil {
			panic(err)
		}

		// err = ioutil.WriteFile(dest2, []byte("["), 0666)
		// if err != nil {
		// 	panic(err)
		// }
		collection.Log.updateLogMetadata("write_status", WRITE_START)
	}

	f1, err := os.OpenFile(dest1, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}

	// f2, err := os.OpenFile(dest2, os.O_APPEND|os.O_WRONLY, 0666)
	// if err != nil {
	// 	panic(err)
	// }

	first := true
	for data := range chnnl {
		uuid := data.Uuid
		collection.Log.updateUuidStatus(uuid, WRITE_START)

		metadata := data.Metadata
		// timeseriesdata := data.TimeseriesData

		if !first {
			f1.Write([]byte(","))
			// f2.Write([]byte(","))
		}
		f1.Write(metadata)
		// f2.Write(timeseriesdata)
		// fmt.Println("Write Complete: ", uuid)
		collection.Log.updateUuidStatus(uuid, WRITE_COMPLETE)
	}

	//TODO: Potential to write too many "]" if crashes here.
    //Maybe have some intermediate status for the file. WRITE_COMPLETE and COMPLETE?
    //Easier if separate logic for metadata and timeseriesdata too.
	f1.Write([]byte("]")) //fix this for WAL
	// f2.Write([]byte("]")) //fix this for WAL
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
    if err != nil {
        fmt.Println("panic 1")
        panic(err)
    }

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
        fmt.Println("panic 2")
		panic(err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
        fmt.Println("panic 3")
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
    q := makeQuery(Url, "select data in (now, now -30d) as ns where uuid='" + collection.Uuids[0] + "'")
    var td *TimeseriesData = new(TimeseriesData)
    json.Unmarshal(q, &td)
    ioutil.WriteFile("testfile.txt", q, 0666)
    fmt.Println("Getting window data")
    fmt.Println(collection.Uuids[0])
    collection.getWindowData(collection.Uuids[110]).getTimeSlots()
    // collection.getTimeseriesCount(collection.Uuids[24])
	// fmt.Println(collection.Log.getUuidStatus(collection.Uuids[0]), UNSTARTED)
}
