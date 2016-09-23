package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
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

type Writer interface { //allows writing from file or from endpoint
    WriteAllUuids() //mainly for logging purposes
    WriteAllMetadata()
    WriteAllTimeseriesData()
}

type DataCollection struct { //need better name
    Url string
	Uuids []string
    Metadatas []Metadata
    TimeseriesDatas []TimeseriesData
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

func NewDataCollection(url string) *DataCollection {
    return &DataCollection {
        Url: url,
        // Uuids: new([]string),
        // Metadatas: new([]Metadata),
        // TimeseriesData: new([]TimeseriesData),
    }
}

func (collection *DataCollection) ReadAllUuids() {
    body := makeQuery(collection.Url, "select distinct uuid")
    json.Unmarshal(body, &(collection.Uuids))
}

func (collection *DataCollection) ReadAllMetadata() { //potentially unnecessary
    collection.Metadatas = make([]Metadata, len(collection.Uuids))
    for i, uuid := range collection.Uuids {
        query := "select * where uuid='" + uuid + "'"
        body := makeQuery(collection.Url, query)
        json.Unmarshal(body, &(collection.Metadatas[i]))
    }
}

func (collection *DataCollection) ReadAllTimeseriesData() { //potentially unnecessary
    collection.TimeseriesDatas = make([]TimeseriesData, len(collection.Uuids))
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
    collection.WriteSomeMetadata(dest, len(collection.Uuids))
}

func (collection *DataCollection) WriteSomeMetadata(dest string, num int) {
    err := ioutil.WriteFile(dest, []byte("["), 0644)
    if err != nil {
        panic(err)
    }
    f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0666)
    if err != nil {
        panic(err)
    }
    for i, uuid := range collection.Uuids[:num] {
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
    collection.WriteSomeTimeseriesData(dest, len(collection.Uuids))
}

func (collection *DataCollection) WriteSomeTimeseriesData(dest string, num int) {
    err := ioutil.WriteFile(dest, []byte("["), 0644)
    if err != nil {
        panic(err)
    }
    f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0666)
    if err != nil {
        panic(err)
    }
    for i, uuid := range collection.Uuids[:num] {
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
    return body
}

func readMetadataFile(index int) (mdata [][]Metadata) { //for test purposes
    file := "metadata_example.txt"
    dat, err := ioutil.ReadFile(file)
    if err != nil {
        panic(err)
    }
    // var mdata [][]Metadata
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
    collection.WriteAllUuids(UuidDestination)
    collection.WriteSomeMetadata(MetadataDestination, 10)
    collection.WriteSomeTimeseriesData(TimeseriesDataDestination, 10)
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
