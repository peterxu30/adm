package main

import (
    // "fmt"
    "log"
    "runtime"
    "time"
)

const (
    Url = "http://128.32.37.201:8079/api/query"
    YearNS = 31536000000000000
)

type ADMManager struct {
    url             string
    uuids           []string
    reader Reader
    writer Writer
    workers *Sema
    openIO *Sema
    log             *Logger
}

func newADMManager(url string, workerSize int, openIO int) *ADMManager {
    log := newLogger()
    reader := newNetworkReader(log) //TODO: dynamically choose appropriate reader/writer types
    writer := newFileWriter(log)
    return &ADMManager{
        url:      url,
        reader: reader,
        writer: writer,
        workers: newSema(workerSize),
        openIO: newSema(openIO),
        log:      log,
    }
}

func (adm *ADMManager) readAllUuids() {
    adm.uuids = adm.reader.readUuids(adm.url)
}

func main() {
    go func() {
        for {
            time.Sleep(1 * time.Second)
            log.Println(runtime.NumGoroutine())
        }
    }()
    adm := newADMManager(Url, 10, 10)
    adm.readAllUuids()
    // fmt.Println(adm.uuids)
}
