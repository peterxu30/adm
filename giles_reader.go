package main

import (
    "encoding/json"
    "bytes"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "strconv"
    "time"
)

const (
    WINDOW_BATCH_SIZE = 10
    METADATA_BATCH_SIZE = 10
    QUERY_TIMEOUT = 10
    QUERY_TRIES = 3
)

type GilesReader struct{}

func newGilesReader() *GilesReader {
    return &GilesReader {}
}

func (r *GilesReader) readUuids(src string) ([]string, *ProcessError) {
    var uuids []string
    body, err := r.makeQuery(src, "select distinct uuid")
    if err != nil {
        return nil, newProcessError(fmt.Sprint("readUuids: read uuids failed err:", err), true, nil)
    }

    err = json.Unmarshal(body, &uuids)
    if err != nil {
        return nil, newProcessError(fmt.Sprint("readUuids: could not unmarshal uuids", uuids, "err:", err), true, nil)
    }

    return uuids, nil
}

func (r *GilesReader) readWindows(src string, uuids []string) ([]*Window, *ProcessError) {
    var windows []*Window
    fmt.Println("readWindows: read windows uuid length", len(uuids))
    uuidsToBatch := make([]string, 0)
    length := len(uuids)
    failed := make([]interface{}, 0)
    for i, uuid := range uuids {
        
        uuidsToBatch = append(uuidsToBatch, uuid)

        if len(uuidsToBatch) == WINDOW_BATCH_SIZE || i == (length - 1) {
            newWindows, err := r.readWindowsBatched(src, uuidsToBatch)
            if err != nil {
                log.Println("readWindows: err:", err)

                for _, uuid := range uuids {
                    window, err := r.readWindow(src, uuid)
                    if err != nil {
                        log.Println("readWindowsBatched: bad uuid", uuid)
                        failed = append(failed, uuid)
                    } else {
                        windows = append(windows, window)
                    }
                }

            } else {
                windows = append(windows, newWindows...)
            }

            uuidsToBatch = make([]string, 0)
        }
    }
    fmt.Println("readWindows len of uuids:", len(uuids), "len of windows:", len(windows))

    if len(failed) > 0 {
        return windows, newProcessError("readWindows: some UUIDs could not be processed", false, failed)
    }
    return windows, nil
}

func (r *GilesReader) readWindowsBatched(src string, uuids []string) ([]*Window, error) {
    var windows []*Window
    query := "select window(365d) data in (0, now) where uuid ="

    query = r.composeBatchQuery(query, uuids)
    body, err := r.makeQuery(src, query)
    if err != nil {
        fmt.Println("bad query string:", query)
        return nil, fmt.Errorf("readWindowsBatched: query failed for uuids:", uuids, "err:", err)
    }
    err = json.Unmarshal(body, &windows)

    if err != nil {
        return nil, fmt.Errorf("readWindowsBatched: batch window read failed for uuids:", uuids, "query", query, "response:", string(body))
    }

    return windows, nil
}

func (r *GilesReader) readWindow(src string, uuid string) (*Window, error) {
    var window *Window
    query := "select window(365d) data in (0, now) where uuid = '" + uuid + "'"
    body, err := r.makeQuery(src, query)
    if err != nil {
        return nil, fmt.Errorf("readWindow: query failed for uuid:", uuid, "err:", err)
    }

    var windows [1]*Window
    err = json.Unmarshal(body, &windows)
    if err != nil {
        return nil, fmt.Errorf("readWindow: could not unmarshal uuid:", uuid, "err:", err)
    }

    window = windows[0]
    return window, nil
}

func (r *GilesReader) readMetadata(src string, uuids []string, dataChan chan *MetadataTuple) *ProcessError {
    uuidsToBatch := make([]string, 0)
    length := len(uuids)
    failed := make([]interface{}, 0)
    for i, uuid := range uuids {
        uuidsToBatch = append(uuidsToBatch, uuid)

        if len(uuidsToBatch) == METADATA_BATCH_SIZE || i == (length - 1) {
            body, err := r.readMetadataBatched(src, uuidsToBatch)
            
            if err != nil {
                log.Println("readMetadataBatched: could not unmarshal uuids:", uuidsToBatch, "err:", err)
                for _, uuid := range uuids {
                    singleBody, err := r.readSingleMetadata(src, uuid)
                    if err != nil {
                        log.Println("readMetadataBatched: bad uuid", uuid)
                        failed = append(failed, uuid)
                    } else {
                        dataChan <- r.makeMetadataTuple([]string{uuid}, singleBody)
                    }
                }
            } else {
                dataChan <- r.makeMetadataTuple(uuidsToBatch, body)
            }

            uuidsToBatch = make([]string, 0)
        }
    }
    close(dataChan)

    if len(failed) > 0 {
        return newProcessError(fmt.Sprint("readMetadata: could not read uuids:", failed), false, failed)
    }
    return nil
}

//helper function
func (r *GilesReader) readMetadataBatched(src string, uuids []string) ([]byte, error) {
    query := "select * where uuid ="

    query = r.composeBatchQuery(query, uuids)
    body, err := r.makeQuery(src, query)
    if err != nil {
        fmt.Println("bad query string:", query)
        return nil, fmt.Errorf("readMetadataBatched: query failed for uuids:", uuids, "err:", err)
    }

    var metadata []*Metadata
    err = json.Unmarshal(body, &metadata)
    if err != nil {
        return nil, fmt.Errorf("readMetadataBatched: could not unmarshal uuids:", uuids, "err:", err)
    } else {
        return body, nil
    }
}

//helper function
func (r *GilesReader) readSingleMetadata(src string, uuid string) ([]byte, error) {
    query := "select * where uuid =" + uuid
    body, err := r.makeQuery(src, query)
    if err != nil {
        return nil, fmt.Errorf("readSingleMetadata: query failed for uuid:", uuid, "err:", err)
    }

    var metadata []*Metadata
    err = json.Unmarshal(body, &metadata)
    if err != nil {
        return nil, fmt.Errorf("readSingleMetadata: could not unmarshal uuid:", uuid, "err:", err)
    }
    return body, nil
}

func (r *GilesReader) readTimeseriesData(src string, slots []*TimeSlot, dataChan chan *TimeseriesTuple) (*ProcessError) {
    failed := make([]interface{}, 0)
    for _, slot := range slots {
        startTime := strconv.FormatInt(slot.StartTime, 10) + "ns"
        endTime := strconv.FormatInt(slot.EndTime, 10) + "ns"

        if endTime == "-1ns" {
            endTime = "now"
        }

        log.Println("readTimeseriesData: making query for uuid", slot.Uuid, slot.StartTime, slot.EndTime)
        query := "select data in (" + startTime + ", " + endTime + ") as ns where uuid='" + slot.Uuid + "'"
        log.Println("readTimeseriesData: query string:", query)
        body, err := r.makeQuery(src, query)
        log.Println("readTimeseriesData: query complete for uuid", slot.Uuid, slot.StartTime, slot.EndTime)
        
        if err != nil {
            fmt.Println("bad query string:", query)
            log.Println("readTimeseriesData: query failed for uuid:", slot.Uuid, "err:", err)
            failed = append(failed, slot)
            continue
        }

        var timeseries []*TimeseriesData
        err = json.Unmarshal(body, &timeseries)
        fmt.Println("len of channel:", len(dataChan))
        if err != nil {
            log.Println("readTimeseriesData: could not unmarshal slot:", slot.Uuid, "query:", query, "err:", err)
            fmt.Println("readTimeseriesData: bad data", string(body))
            failed = append(failed, slot)
            continue         
        } else {
            log.Println("readTimeseriesData: inserting uuid", slot.Uuid, "into channel")
            dataChan <- r.makeTimeseriesTuple(slot, body)
            log.Println("readTimeseriesData: insert complete for uuid", slot.Uuid, "into channel")            
        }
        log.Println("readTimeseriesData: read uuid", slot.Uuid)
    }
    close(dataChan)
    log.Println("readTimeseriesData: finished read")

    if len(failed) > 0 {
        return newProcessError(fmt.Sprint("readTimeseriesData: could not read slots:", failed), false, failed)
    }

    return nil
}

/* General purpose function to make an HTTP POST request to the specified url
 * with the specified queryString.
 * Return value is of type []byte. It is up to the calling function to convert
 * []byte into the appropriate type.
 */
func (r *GilesReader) makeQuery(url string, queryString string) ([]byte, error) {
    var body []byte
    var err error
    for i := 0; i < QUERY_TRIES; i++ {
        // t := time.Duration(QUERY_TIMEOUT + (5 * i))
        time.Sleep(5 * time.Second)
        t := time.Duration(QUERY_TIMEOUT * (i + 1))
        query := []byte(queryString)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(query))
        if err != nil {
            body, err = nil, fmt.Errorf("makeQuery: could not create new request to", url, "for", queryString, "err:", err)
            continue
        }

        timeout := time.Duration(t * time.Second)
        client := &http.Client{
            Timeout: timeout,
        }
        resp, err := client.Do(req)
        if err != nil {
            body, err = nil, fmt.Errorf("makeQuery: failed to execute request to", url, "for", queryString, "err:", err)
            continue
        }

        defer resp.Body.Close()
        body, err = ioutil.ReadAll(resp.Body)
        if err != nil {
            body, err = nil, fmt.Errorf("makeQuery: failed to read response body from", url, "for", queryString, "err:", err)
            continue
        }
        return body, err
    }
    return body, err
}

func (r *GilesReader) composeBatchQuery(query string, uuids []string) string {
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

func (r *GilesReader) makeMetadataTuple(uuids []string, data []byte) *MetadataTuple {
    return &MetadataTuple {
        uuids: uuids,
        data: data,
    }
}

func (r *GilesReader) makeTimeseriesTuple(slot *TimeSlot, data []byte) *TimeseriesTuple {
    return &TimeseriesTuple {
        slot: slot,
        data: data,
    }
}
