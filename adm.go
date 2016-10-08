package main

import (
	"bytes"
	"encoding/json"
	"fmt"
    // "github.com/boltdb/bolt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
    "sync"
	// "strings"
)

const (
    Url = "http://castle.cs.berkeley.edu:8079/api/query"
    UuidDestination = "uuids.txt"
    MetadataDestination = "metadata.txt"
    TimeseriesDataDestination = "timeseriesdata.txt"
)

type Reader interface { //potentially unnecessary. no point in storing all of this information in memory is there?
    ReadAllUuids() //this is necessary
    ReadAllMetadata()
    ReadAllTimeseriesData()
}

type Writer interface { //allows writing to file or to endpoint
    WriteAllUuids(dest string) //mainly for logging purposes
    WriteAllMetadata(dest string) //dest can be a url or a file name
    WriteSomeMetadata(dest string, start int, end int)
    WriteAllTimeseriesData(dest string)
    WriteSomeTimeseriesData(dest string, start int, end int)
}

type Metadata struct {
    Path string
    Uuid string `json:"uuid"`
    Properties interface{}
    Metadata interface{}
}

type TimeseriesData struct {
    Uuid string
    Readings interface{} //array of string arrays
}

type CombinedData struct { //final form of data to send out
    Path string
    Uuid string `json:"uuid"`
    Properties interface{}
    Metadata interface{}
    Readings interface{}
}

type UuidTuple struct {
    Uuid string //for logging
    Metadata []byte
    TimeseriesData []byte
}

type DataCollection struct { //need better name
    Log *Logger
    Wg sync.WaitGroup
    Url string
	Uuids []string
    Metadatas [][]Metadata
    TimeseriesDatas [][]TimeseriesData
    DataChan chan UuidTuple
}

func NewDataCollection(url string) *DataCollection {
    return &DataCollection {
        Log: newLogger(),
        Url: url,
        DataChan: make(chan UuidTuple),
        // Uuids: new([]string),
        // Metadatas: new([]Metadata),
        // TimeseriesData: new([]TimeseriesData),
    }
}

func (collection *DataCollection) ReadAllUuids() {
    body := makeQuery(collection.Url, "select distinct uuid")
    json.Unmarshal(body, &(collection.Uuids))
}

/* Uses go routines */
func (collection *DataCollection) AddAllUuidsToLog() {
    fmt.Println("addalluuidstolog")
    if collection.Log.getLogMetadata("read_uuids") == WRITE_COMPLETE {
        fmt.Println("UUID Log write complete")
        return
    }
    length := len(collection.Uuids)
    numRoutinesFloat := float64(length) / float64(2000)
    numRoutines := length / 2000
    if numRoutinesFloat > float64(numRoutines) {
        numRoutines++
    }
    start := 0
    end := 2000
    if numRoutines == 0 {
        numRoutines = 1
        end = length
    }
    fmt.Println("Num routines AddAllUuidsToLog", numRoutines)
    // collection.Wg.Add(numRoutines)
    var wg sync.WaitGroup
    wg.Add(numRoutines)
    for numRoutines > 0 {
        go collection.AddUuidsToLog(start, end, &wg)
        start = end
        end += 2000
        if end > length {
            end = length
        }
        numRoutines--;
    }
    // collection.Wg.Wait()
    wg.Wait()
    fmt.Println("Read uuids complete")
    fmt.Println("12000: ", collection.Log.getUuidStatus(collection.Uuids[12000]))
    collection.Log.updateLogMetadata("read_uuids", WRITE_COMPLETE)
}

/* Called as a go routine */
func (collection *DataCollection) AddUuidsToLog(start int, end int, wg *sync.WaitGroup) {
    defer wg.Done()
    for i := start; i < end; i++ {
        uuid := collection.Uuids[i]
        collection.Log.updateUuidStatus(uuid, UNSTARTED)
    }
    fmt.Println("Uuids ", start, " to ", end, " added to log")
}

func (collection *DataCollection) ReadAllMetadata() { //potentially unnecessary
    collection.Metadatas = make([][]Metadata, len(collection.Uuids))
    for i, uuid := range collection.Uuids {
        query := "select * where uuid='" + uuid + "'"
        body := makeQuery(collection.Url, query)
        json.Unmarshal(body, &(collection.Metadatas[i]))
    }
}

func (collection *DataCollection) ReadAllTimeseriesData() { //potentially unnecessary
    collection.TimeseriesDatas = make([][]TimeseriesData, len(collection.Uuids))
    for i, uuid := range collection.Uuids {
        query := "select data before now as ns where uuid='" + uuid +"'"
        body := makeQuery(collection.Url, query)
        json.Unmarshal(body, &(collection.TimeseriesDatas[i]))
    }
}

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

func (collection *DataCollection) WriteAllMetadata(dest string) {
    collection.Wg.Add(1)
    go collection.WriteSomeMetadata(dest, 0, len(collection.Uuids))
}

/* Should only be called as a go routine */
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
        if i % 50 == 0 {
            length := strconv.Itoa(i)
            fmt.Println(length + " ids processed.")
        }
        query := "select * where uuid='" + uuid + "'"
        body := makeQuery(collection.Url, query)
        f.Write(body)
    }
    f.Write([]byte("]"))
}

func (collection *DataCollection) WriteAllTimeseriesData(dest string) {
    collection.Wg.Add(1)
    go collection.WriteSomeTimeseriesData(dest, 0, len(collection.Uuids))
}

/* Should only be called as a go routine */
func (collection *DataCollection) WriteSomeTimeseriesData(dest string, start int, end int) {
    defer collection.Wg.Done()
    fmt.Println("Writing timeseriesdata to channel\n")
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
        if i % 50 == 0 {
            length := strconv.Itoa(i)
            fmt.Println(length + " ids processed.")
        }
        query := "select data before now as ns where uuid='" + uuid + "'"
        body := makeQuery(collection.Url, query)
        f.Write(body)
    }
    f.Write([]byte("]"))
}

func (collection *DataCollection) WriteAllDataBlocks(metaDest string, timeseriesDest string) {
    //TODO: need to add "[" to front of metadata and timeseries files
    //TODO: need to add "]" to end of metadata and timeseries files
    defer close(collection.DataChan)
    length := len(collection.Uuids)
    numRoutinesFloat := float64(length) / float64(2000)
    numRoutines := length / 2000
    if numRoutinesFloat > float64(numRoutines) {
        numRoutines++
    }
    start := 0
    end := 2000
    if numRoutines == 0 {
        numRoutines = 1
        end = length
    }
    collection.Wg.Add(numRoutines)
    go collection.WriteFromChannel(metaDest, timeseriesDest, collection.DataChan)
    for numRoutines > 0 {
        go collection.WriteDataBlock(metaDest, timeseriesDest, start, end) //writes to channel
        // go routine to WriteFromChannel
        start = end
        end += 2000
        if end > length {
            end = length
        }
        numRoutines--;
    }
    collection.Wg.Wait()
    //close channel here
}

/* Should only be called as a go routine */
/* CANNOT CONCURRENTLY WRITE TO SAME FILE. USE CHANNELS */
func (collection *DataCollection) WriteDataBlock(metaDest string, timeseriesDest string, start int, end int) {
    defer collection.Wg.Done()
    // f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0666)
    // if err != nil {
        // panic(err)
    // }
    for i := start; i < end; i++ {
        uuid := collection.Uuids[i]

        status := collection.Log.getUuidStatus(uuid)

        if status == WRITE_COMPLETE {
            // fmt.Println("UUID already written")
            continue
        }
        
        //read into channel
        mQuery := "select * where uuid='" + uuid + "'"
        mBody := makeQuery(collection.Url, mQuery)

        tQuery := "select data before now as ns where uuid='" + uuid + "'"
        tBody := makeQuery(collection.Url, tQuery)

        tuple := UuidTuple {
            Uuid: uuid,
            Metadata: mBody,
            TimeseriesData: tBody,
        }

        collection.DataChan <- tuple

        // if status == UNSTARTED || status == READ_START {
        //     collection.Log.updateUuidStatus(uuid, READ_START)
        //     //read logic goes here
        //     collection.Log.updateUuidStatus(uuid, READ_COMPLETE)
        // }

        // status = collection.Log.getUuidStatus(uuid)
        // if status == READ_COMPLETE || status == WRITE_START {
        //     collection.Log.updateUuidStatus(uuid, WRITE_START)
        //     //write logic goes here
        //     collection.WriteSomeMetadata(metaDest, i, i+1)
        //     collection.WriteSomeTimeseriesData(timeseriesDest, i, i+1)
        //     collection.Log.updateUuidStatus(uuid, WRITE_COMPLETE)
        // }
    }
    fmt.Println("Block ", start, " - ", end, " read complete")
}

/* Should only be called once in a go routine 
 * dest1 - metadata
 * dest2 - timeseries
 * Does not check if UUID was already written. This is responsibility of the WriteDataBlock method.
 */
func (collection *DataCollection) WriteFromChannel(dest1 string, dest2 string, chnnl chan UuidTuple) {
    // defer collection.Wg.Done()
    writeStatus := collection.Log.getLogMetadata("write_status")

    if writeStatus == WRITE_COMPLETE {
        fmt.Println("Files ", dest1, ", ", dest2, " already written.")
        return
    }
 
    if writeStatus == UNSTARTED {
        err := ioutil.WriteFile(dest1, []byte("["), 0666) //fix this for WAL
        if err != nil {
            panic(err)
        }

        err = ioutil.WriteFile(dest2, []byte("["), 0666) //fix this for WAL
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

    f1.Write([]byte("]")) //fix this for WAL
    f2.Write([]byte("]")) //fix this for WAL
    collection.Log.updateLogMetadata("write_status", WRITE_COMPLETE)
}


func makeQuery(url string, queryString string) (uuids []byte) {
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
    // fmt.Println("Query: ", string(body))
    return body
}

func readMetadataFile(index int) (mdata [][]Metadata) { //for test purposes
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
    fmt.Println("UUID Length: ", len(collection.Uuids))
    collection.WriteAllDataBlocks(MetadataDestination, TimeseriesDataDestination)
    // collection.WriteAllMetadata(MetadataDestination)
    // collection.WriteAllTimeseriesData(TimeseriesDataDestination)
    // collection.Wg.Add(3)
    // go collection.WriteAllUuids(UuidDestination)
    // go collection.WriteSomeMetadata(MetadataDestination, 0, 10)
    // go collection.WriteSomeTimeseriesData(TimeseriesDataDestination, 0, 10)
    fmt.Println("Done\n")
    fmt.Println(collection.Log.getUuidStatus(collection.Uuids[0]), UNSTARTED)
    // collection.ReadAllMetadata()
    // fmt.Println(collection.Metadatas)
    // collection.ReadAllTimeseriesData()
    // fmt.Println(collection.TimeseriesDatas)
    // body := makeQuery(Url, "select data before now as ns where uuid='79fc7217-7795-58d0-8deb-e2353f88b4f8'")// where uuid='62710721-5135-56d5-8d38-f1d3e9f87f70'")
    // ioutil.WriteFile("data_example.txt", body, 0644)

    // timedata := new([]TimeseriesData)
    // json.Unmarshal(body, &timedata)
    // fmt.Println("TIME DATA: ", (*timedata)[0].Readings.([]interface{})[0].([]interface{})[0]) //accessing data point 1, value 1
	//log uuids
 //    fmt.Println("Recording UUIDs")
	// uuidByte := makeQuery(Url, "select distinct uuid")
	// ioutil.WriteFile("uuids1.txt", uuidByte, 0644)

 //    var uuids []string
 //    json.Unmarshal(uuidByte, &uuids)
 //    length := strconv.Itoa(len(uuids))
 //    fmt.Println("Recording metadata: " + length + " files to process")
 //    ioutil.WriteFile("metadata_example.txt", []byte("["), 0644)
 //    f, _ := os.OpenFile("metadata_example.txt", os.O_APPEND|os.O_WRONLY, 0666)
 //    for i, uuid := range uuids {
 //        if i < 100 {
 //        if i > 0 {
 //            f.Write([]byte(","))
 //        }
 //        if i % 50 == 0 {
 //            length1 := strconv.Itoa(i)
 //            fmt.Println(length1 + " files processed.")
 //        }
 //        query := "select * where uuid='" + uuid + "'"
 //        mbody := makeQuery(Url, query)
 //        f.Write(mbody)
 //        }
 //    }
 //    f.Write([]byte("]"))
    // readMetadataFile(0)


	// body := makeQuery(Url, "select * where uuid='8aa76954-d33d-5a03-93b1-cd07a67aa7ff'")

    // var ids []string
    // var a [2]json.RawMessage
    // var a = new(interface{})
    // var a interface{}
    // json.Unmarshal(body, &ids)
    // metadata2test := new([]map[string]interface{})
    // mdata := new([]Metadata)
    // dec := json.NewDecoder(bytes.NewBuffer(body))
    // if err := dec.Decode(&mdata); err != nil {
        // return fmt.Errorf("decode person: %v", err)
        // fmt.Println(err)
    // }

    // Once decoded we can access the fields by name.
    // fmt.Println("TEST: ", (*mdata)[0].Path)
    // fmt.Println("TEST: ", (*mdata)[0].Uuid)
    // fmt.Println("TEST: ", (*mdata)[0].Properties)//["UnitofTime"]) need to cast Properties.(map[string]interface{}) to access
    // fmt.Println("TEST: ", (*mdata)[0].Metadata)

    // json.Unmarshal(body, &mdata)
    // fmt.Println(mdata)
    // json.Unmarshal(body, &a[0])
    // fmt.Println(string(a[0]))
    // fmt.Println(ids)
    // newJson := new(Metadata)
    // newJson.Uuid = "8aa76954-d33d-5a03-93b1-cd07a67aa7ff"
    // newJson.Data = string(a[0][1:len(a[0])-1])
    // fmt.Println("New Json: ", newJson)
    // bits, _ := json.Marshal(newJson)
    // testJson := new(Metadata)
    // json.Unmarshal(bits, testJson)
    // fmt.Println("Test json: ", testJson.Data)
    // // ioutil.WriteFile("metadata_example.txt", bits, 0644)
    // f, _ := os.OpenFile("metadata_example.txt", os.O_APPEND|os.O_WRONLY, 0666)
    // f.Write(bits)
    // f.Write([]byte("]"))
/////////
//     query = []byte("select * where uuid='042f56d4-37f8-5d85-a2c1-c3e5aebf94ff'") //metadata retrieval
// 	req, err = http.NewRequest("POST", url, bytes.NewBuffer(query))

// 	// client = &http.Client{}
//     resp, err = client.Do(req)
//     if err != nil {
//         panic(err)
//     }

//     defer resp.Body.Close()
//     fmt.Println("response Status:", resp.Status)
//     fmt.Println("response Headers:", resp.Header)
//     body1, _ := ioutil.ReadAll(resp.Body)
//  //   	// ids = []string
//     // json.Unmarshal(body1, &ids)
//     json.Unmarshal(body1, &a[1])
//     // fmt.Println(string(a[1]))
//     newJson = new(Metadata)
//     newJson.Uuid = "042f56d4-37f8-5d85-a2c1-c3e5aebf94ff"
//     newJson.Data = string(a[1][1:len(a[1])-1])
//     bits, err = json.Marshal(newJson)
//     testJson = new(Metadata)
//     json.Unmarshal(bits, testJson)
//     fmt.Println("Test json: ", testJson.Data)
//     // fmt.Println(ids)
//     // ioutil.WriteFile("metadata_example.txt", body, 0644)
//     // f, err := os.OpenFile("metadata_example.txt", os.O_APPEND|os.O_WRONLY, 0666)
//     // f.Write(bits)



// //////////test read
//     shit, err := ioutil.ReadFile("metadata_example.txt")
//     aa := new(Metadata)
//     json.Unmarshal(shit, &aa)
//     fmt.Println("Hello, ", aa.Uuid)
}
