package main

import (
    "encoding/json"
    "bytes"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "strconv"
)

const (
    WindowBatchSize = 10
    MetadataBatchSize = 10
)

type NetworkReader struct{}

func newNetworkReader() *NetworkReader {
    return &NetworkReader {}
}

func (r *NetworkReader) readUuids(src string) (uuids []string, err error) {
    body, err := r.makeQuery(src, "select distinct uuid")
    if err != nil {
        err = fmt.Errorf("readUuids: read uuids failed err:", err)
        return
    }

    err = json.Unmarshal(body, &uuids)
    if err != nil {
        err = fmt.Errorf("readUuids: could not unmarshal uuids", uuids, "err:", err)
        return
    }

    return
}

func (r *NetworkReader) readWindows(src string, uuids []string) (windows []*Window, err error) {
    fmt.Println("readWindows: read windows uuid length", len(uuids))
    // windows := make([]*Window, 0)
    uuidsToBatch := make([]string, 0)
    length := len(uuids)
    for i, uuid := range uuids {
        
        uuidsToBatch = append(uuidsToBatch, uuid)

        if len(uuidsToBatch) == WindowBatchSize || i == (length - 1) {
            newWindows, err := r.readWindowsBatched(src, uuidsToBatch)
            if err != nil {
                // return nil, errors.New("readWindows: err:", err)
                log.Println("readWindows: err:", err)
            } else {
                windows = append(windows, newWindows...)
            }
            // fmt.Println("readWindows: length of uuidsToBatch | newWindows:", len(uuidsToBatch), len(newWindows))
            // if len(uuidsToBatch) != len(newWindows) {
            //     fmt.Println("\nreadWindows: len mismatch, uuidsToBatch", len(uuidsToBatch), len(newWindows))
            //     fmt.Println(uuidsToBatch)
            //     for _, win := range newWindows {
            //         fmt.Println("window:", win.Uuid, length)
            //     }
            // }
            
            windows = append(windows, newWindows...)
            uuidsToBatch = make([]string, 0)
        }
    }

    return
}

func (r *NetworkReader) readWindowsBatched(src string, uuids []string) (windows []*Window, err error) {
    // fmt.Println("readWindowsBatched: batching", len(uuids), "windows", uuids)
    // windows := make([]*Window, 0)
    query := "select window(365d) data in (0, now) where uuid ="

    query = r.composeBatchQuery(query, uuids)
    body, err := r.makeQuery(src, query)
    if err != nil {
        return nil, fmt.Errorf("readWindowsBatched: query failed for uuids:", uuids, "err:", err)
    }
    err = json.Unmarshal(body, &windows)

    // if (len(windows) != len(uuids)) {
    //     fmt.Println("len mismatch query:")
    //     fmt.Println(query)
    // }

    if err != nil {
        // log.Println("readWindowsBatched: batch window read failed for query:", query, "\n reason:", string(body))
        return nil, fmt.Errorf("readWindowsBatched: could not unmarshal uuids:", uuids, "err:", err)
    }

    return
}

func (r *NetworkReader) readWindow(src string, uuid string) (window *Window, err error) {
    query := "select window(365d) data in (0, now) where uuid = '" + uuid + "'"
    body, err := r.makeQuery(src, query)
    if err != nil {
        return nil, fmt.Errorf("readWindow: query failed for uuid:", uuid, "err:", err)
    }

    var windows [1]*Window
    err = json.Unmarshal(body, &windows)
    if err != nil {
        // log.Println("readWindow: window", uuid, "could not be read \n reason:", err)
        return nil, fmt.Errorf("readWindow: could not unmarshal uuid:", uuid, "err:", err)
    }

    window = windows[0]
    return
}

func (r *NetworkReader) readMetadata(src string, uuids []string, dataChan chan *MetadataTuple) (err error) {
    uuidsToBatch := make([]string, 0)
    length := len(uuids)
    for i, uuid := range uuids {
        uuidsToBatch = append(uuidsToBatch, uuid)

        if len(uuidsToBatch) == MetadataBatchSize || i == (length - 1) {
            err := r.readMetadataBatched(src, uuidsToBatch, dataChan)
            if err != nil {
                // log.Println("readMetadata: err:", err)
                return fmt.Errorf("readMetadata: err:", err)
            }
            uuidsToBatch = make([]string, 0)
        }
    }
    close(dataChan)
    return
}

//helper function
func (r *NetworkReader) readMetadataBatched(src string, uuids []string, dataChan chan *MetadataTuple) (err error) {
    query := "select * where uuid ="

    var uuidsToBatch []string
    toBatchCount := 0
    for _, uuid := range uuids {
        uuidsToBatch = append(uuidsToBatch, uuid)
        toBatchCount++
    }

    if toBatchCount > 0 {
        query = r.composeBatchQuery(query, uuidsToBatch)
        body, err := r.makeQuery(src, query)
        if err != nil {
            // log.Println("readMetadataBatched: query failed for uuids:", uuids, "err:", err)
            return fmt.Errorf("readMetadataBatched: query failed for uuids:", uuids, "err:", err)
        }

        var metadata []*Metadata
        err = json.Unmarshal(body, &metadata)
        if err != nil {
            return fmt.Errorf("readMetadataBatched: could not unmarshal uuids:", uuids, "err:", err)
        }
        
        dataChan <- r.makeMetadataTuple(uuidsToBatch, body)
    }
    return
}

func (r *NetworkReader) readTimeseriesData(src string, slots []*TimeSlot, dataChan chan *TimeseriesTuple) (err error) {
    for _, slot := range slots {
        startTime := strconv.FormatInt(slot.StartTime, 10)
        endTime := strconv.FormatInt(slot.EndTime, 10)
        query := "select data in (" + startTime + ", " + endTime + ") as ns where uuid='" + slot.Uuid + "'"
        body, err := r.makeQuery(src, query)
        
        if err != nil {
            log.Println("readTimeseriesData: query failed for uuid:", slot.Uuid, "err:", err)
        }

        //TODO: UNTESTED
        var timeseries []*TimeseriesData
        err = json.Unmarshal(body, &timeseries)
        if err != nil {
            // log.Println("readTimeseriesData: could not unmarshal slot:", slot.Uuid, "err:", err)
            return fmt.Errorf("readTimeseriesData: could not unmarshal slot:", slot.Uuid, "err:", err)
        }

        dataChan <- r.makeTimeseriesTuple(slot, body)
    }
    close(dataChan)
    return
}

/* General purpose function to make an HTTP POST request to the specified url
 * with the specified queryString.
 * Return value is of type []byte. It is up to the calling function to convert
 * []byte into the appropriate type.
 */
func (r *NetworkReader) makeQuery(url string, queryString string) (body []byte, err error) {
    query := []byte(queryString)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(query))
    if err != nil {
        // log.Println("makeQuery: could not create new request to", url, "for", queryString)
        return nil, fmt.Errorf("makeQuery: could not create new request to", url, "for", queryString, "err:", err)
    }

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        // log.Println("makeQuery: failed to execute request to", url, "for", queryString)
        return nil, fmt.Errorf("makeQuery: failed to execute request to", url, "for", queryString, "err:", err)
    }

    defer resp.Body.Close()
    body, err = ioutil.ReadAll(resp.Body)
    if err != nil {
        // log.Println("makeQuery: failed to read response body from", url, "for", queryString)
        return nil, fmt.Errorf("makeQuery: failed to read response body from", url, "for", queryString, "err:", err)
    }
    return
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
