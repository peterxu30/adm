package main

import (
    "encoding/json"
    "bytes"
    "fmt"
    "io/ioutil"
    "net/http"
)

type NetworkReader struct {
    queryUrl string
    log *Logger
}

func newNetworkReader(queryUrl string, log *Logger) *NetworkReader {
    return &NetworkReader {
        queryUrl: queryUrl,
        log: log,
    }
}

func (r *NetworkReader) readUuids() []string {
    if (r.log.getLogMetadata(UUIDS_FETCHED) == WRITE_COMPLETE) {
        fmt.Println("uuids were previously read")
        return r.log.getUuidMetadataKeySet()
    }
    var uuids []string
    body := r.makeQuery(r.queryUrl, "select distinct uuid")
    json.Unmarshal(body, &uuids)

    for _, uuid := range uuids {
        r.log.updateWindowStatus(uuid, nil)
        r.log.updateUuidMetadataStatus(uuid, NOT_STARTED)
    }
    r.log.updateLogMetadata(UUIDS_FETCHED, WRITE_COMPLETE)
    return uuids
}

func (r *NetworkReader) readMetadata(uuids []string, metadataChan chan DataTuple) {
    for _, uuid := range uuids {
        if r.log.getUuidMetadataStatus(uuid) == WRITE_COMPLETE {
            continue
        }
        r.log.updateUuidMetadataStatus(uuid, WRITE_START)
        mQuery := "select * where uuid='" + uuid + "'"
        mBody := r.makeQuery(r.queryUrl, mQuery)
        mTuple := DataTuple {
            uuid: uuid,
            data: mBody,
        }
        metadataChan <- mTuple
    }
}

func (r *NetworkReader) readTimeseriesData(start *TimeSlot, fullUuids []string, end *TimeSlot, timeseriesChan chan DataTuple) {

}

/* General purpose function to make an HTTP POST request to the specified url
 * with the specified queryString.
 * Return value is of type []byte. It is up to the calling function to convert
 * []byte into the appropriate type.
 */
func (r *NetworkReader) makeQuery(url string, queryString string) []byte {
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
