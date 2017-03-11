package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

func getDirPath(path string) (dp string) {
    fldr_lst := strings.Split(path, "/")
    fldr_lst = fldr_lst[:len(fldr_lst) - 1]
    return concatDirPath(fldr_lst)
}

func concatDirPath(path []string) (dp string) {
    for _, str := range path {
        dp += str + "/"
    }
    return
}

/*
cite: http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
*/
func fileExists(file string) bool {
	if _, err := os.Stat(file); err == nil {
		return true
	}
	return false
}

/* General purpose function to make an HTTP POST request to the specified url
 * with the specified queryString.
 * Return value is of type []byte. It is up to the calling function to convert
 * []byte into the appropriate type.
 */
func makeQuery(url string, queryString string) ([]byte, error) {
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

func composeBatchQuery(query string, uuids []string) string {
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

func makeMetadataTuple(uuids []string, data []byte) *MetadataTuple {
    return &MetadataTuple {
        uuids: uuids,
        data: data,
    }
}

func makeTimeseriesTuple(slot *TimeSlot, data []byte) *TimeseriesTuple {
    return &TimeseriesTuple {
        slot: slot,
        data: data,
    }
}
