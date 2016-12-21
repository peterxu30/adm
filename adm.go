package main

import (
    "log"
    "runtime"
    "time"
)

const (
    Url = "http://128.32.37.201:8079/api/query"
    YearNS = 31536000000000000
)

type ADMManager struct {
    log             *Logger
    url             string
    uuids           []string
    reader Reader
    writer Writer
    workers *Sema
    openFiles *Sema
}

func newADMManager(url string, workerSize int, openFileSize int) *ADMManager {
    log := newLogger()
    reader := newNetworkReader(url, log) //TODO: dynamically choose appropriate reader/writer types
    writer := newFileWriter()
    return &ADMManager{
        log:      log,
        url:      url,
        reader: reader,
        writer: writer,
        workers: newSema(workerSize),
        openFiles: newSema(openFileSize),
    }
}

func (adm *ADMManager) readAllUuids() {
    adm.uuids = adm.reader.readUuids()
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
}
