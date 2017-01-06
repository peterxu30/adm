package main

import (
    "encoding/json"
    "bytes"
    "fmt"
    "io/ioutil"
    "net/http"
    "strconv"
)

const (
    WindowBatchSize = 10
    MetadataBatchSize = 10
)

type NetworkReader struct {
    log *Logger
}

func newNetworkReader(log *Logger) *NetworkReader {
    return &NetworkReader {
        log: log,
    }
}

func (r *NetworkReader) readUuids(src string) []string {
    if (r.log.getLogMetadata(UUIDS_FETCHED) == WRITE_COMPLETE) {
        fmt.Println("uuids were previously read")
        return r.log.getUuidMetadataKeySet()
    }

    var uuids []string
    body := r.makeQuery(src, "select distinct uuid")
    json.Unmarshal(body, &uuids)

    for _, uuid := range uuids {
        r.log.updateUuidMetadataStatus(uuid, NOT_STARTED)
    }

    return uuids
}

func (r *NetworkReader) readWindows(src string, uuids []string) []*Window {
    fmt.Println("read windows uuid length", len(uuids))
    windows := make([]*Window, 0) //TODO: possible bug?
    uuidsToBatch := make([]string, 0)
    length := len(uuids)
    for i, uuid := range uuids {
        
        uuidsToBatch = append(uuidsToBatch, uuid)

        if len(uuidsToBatch) == WindowBatchSize || i < (length - 1) {}
            newWindows := r.readWindowsBatched(src, uuidsToBatch)
            fmt.Println(len(uuidsToBatch), len(newWindows))
            windows = append(windows, newWindows...)
            uuidsToBatch = make([]string, 0)
        }
    }

    return windows
}

func (r *NetworkReader) readWindowsBatched(src string, uuids []string) []*Window {
    fmt.Println("batching", len(uuids), "windows")
    windows := make([]*Window, 0)
    query := "select window(365d) data in (0, now) where uuid ="

    var readBefore []*Window
    var uuidsToBatch []string
    toBatchCount := 0
    for _, uuid := range uuids {
        w := r.log.getWindowStatus(uuid)
        if w != nil {
            readBefore = append(readBefore, w)
            continue
        } else {
            uuidsToBatch = append(uuidsToBatch, uuid)
            toBatchCount++
        }
    }

    if toBatchCount > 0 {
        query = r.composeBatchQuery(query, uuidsToBatch)
        body := r.makeQuery(src, query)
        err := json.Unmarshal(body, &windows)
        if err != nil {
            fmt.Println("batch window read failed")
            fmt.Println(body)
        }

        for _, window := range windows {
            fmt.Println(window)
            if window == nil {
                fmt.Println("nil window")
                continue
            }
            r.log.updateWindowStatus(window.Uuid, window)
        }
    }

    for _, window := range readBefore {
        windows = append(windows, window)
    } 

    return windows
}

func (r *NetworkReader) readWindow(src string, uuid string) *Window {
    w := r.log.getWindowStatus(uuid)
    if w != nil {
        return w
    }

    query := "select window(365d) data in (0, now) where uuid = '" + uuid + "'"
    body := r.makeQuery(src, query)

    var windows [1]*Window
    err := json.Unmarshal(body, &windows)
    if err != nil {
        fmt.Println("window", uuid + ":", "could not be read")
        // panic(err)
        return nil
    }
    window := windows[0]
    r.log.updateWindowStatus(uuid, window)
    // fmt.Println(uuid, window)
    return window
}

func (r *NetworkReader) readMetadata(src string, uuids []string, dataChan chan *MetadataTuple) {
    if r.log.getLogMetadata(METADATA_WRITTEN) == WRITE_COMPLETE {
        return
    }

    uuidsToBatch := make([]string, 0)
    length := len(uuids)
    for i, uuid := range uuids {
        if r.log.getUuidMetadataStatus(uuid) == WRITE_COMPLETE {
            continue
        }

        uuidsToBatch = append(uuidsToBatch, uuid)

        if len(uuidsToBatch) == MetadataBatchSize || i == (length - 1) {
            r.readMetadataBatched(src, uuidsToBatch, dataChan)
            uuidsToBatch = make([]string, 0)
        }
    }
    close(dataChan)
}

//helper function
func (r *NetworkReader) readMetadataBatched(src string, uuids []string, dataChan chan *MetadataTuple) {
    query := "select * where uuid ="

    var uuidsToBatch []string
    toBatchCount := 0
    for _, uuid := range uuids {
        w := r.log.getUuidMetadataStatus(uuid)
        if w == WRITE_COMPLETE {
            continue
        } else {
            uuidsToBatch = append(uuidsToBatch, uuid)
            toBatchCount++
        }
    }

    if toBatchCount > 0 {
        query = r.composeBatchQuery(query, uuidsToBatch)
        body := r.makeQuery(src, query)
        dataChan <- r.makeMetadataTuple(uuidsToBatch, body)
    }
}

//TODO: Batch the queries
func (r *NetworkReader) readTimeseriesData(src string, slots []*TimeSlot, dataChan chan *TimeseriesTuple) {
    if r.log.getLogMetadata(TIMESERIES_WRITTEN) == WRITE_COMPLETE {
        return
    }

    for _, slot := range slots {
        if r.log.getUuidTimeseriesStatus(slot) == WRITE_COMPLETE {
            continue
        }

        startTime := strconv.FormatInt(slot.StartTime, 10)
        endTime := strconv.FormatInt(slot.EndTime, 10)
        query := "select data in (" + startTime + ", " + endTime + ") as ns where uuid='" + slot.Uuid + "'"
        body := r.makeQuery(src, query)
        dataChan <- r.makeTimeseriesTuple(slot, body)
    }
    close(dataChan)
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
        fmt.Println("could not create new request")
        panic(err)
    }

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        fmt.Println("failed to execute request")
        panic(err)
    }

    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Println("failed to read response bodyf")
        panic(err)
    }
    return body
}

func (r *NetworkReader) composeBatchQuery(query string, uuids []string) string {
    first := true
    for _, uuid := range uuids {
        if !first {
            query += " or uuid = "
        } else {
            first = false
        }
        query += "'" + uuid + "'"
    }
    return query
}

func (r *NetworkReader) makeMetadataTuple(uuids []string, data []byte) *MetadataTuple {
    return &MetadataTuple {
        uuids: uuids,
        data: data,
    }
}

func (r *NetworkReader) makeTimeseriesTuple(slot *TimeSlot, data []byte) *TimeseriesTuple {
    return &TimeseriesTuple {
        slot: slot,
        data: data,
    }
}
