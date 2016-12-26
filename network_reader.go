package main

import (
    "encoding/json"
    "bytes"
    "fmt"
    "io/ioutil"
    "net/http"
    "strconv"
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
    windows := make([]*Window, len(uuids))
    for i, uuid := range uuids {
        windows[i] = r.readWindow(src, uuid)
    }
    return windows
}

func (r *NetworkReader) readWindowsBatched(src string, uuids []string) []*Window {
    windows := make([]*Window, len(uuids))
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
        }

        for _, window := range windows {
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
    fmt.Println(uuid, window)
    return window
}

//if want to batch queries, modify MetadataTuple to have an array of uuids instead of just one for logging.
func (r *NetworkReader) readMetadata(src string, uuids []string, dataChan chan *MetadataTuple) {
    if r.log.getLogMetadata(METADATA_WRITTEN) == WRITE_COMPLETE {
        return
    }

    for _, uuid := range uuids {
        if r.log.getUuidMetadataStatus(uuid) == WRITE_COMPLETE {
            continue
        }

        query := "select * where uuid='" + uuid + "'"
        body := r.makeQuery(src, query)
        dataChan <- r.makeMetadataTuple(uuid, body)
    }
    close(dataChan)
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

func (r *NetworkReader) makeMetadataTuple(uuid string, data []byte) *MetadataTuple {
    return &MetadataTuple {
        uuid: uuid,
        data: data,
    }
}

func (r *NetworkReader) makeTimeseriesTuple(slot *TimeSlot, data []byte) *TimeseriesTuple {
    return &TimeseriesTuple {
        slot: slot,
        data: data,
    }
}
