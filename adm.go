//TODO: At the moment, the log is checked at every level to see if a job was done or not. After adm methods are better flushed out, go back and consider where logging is most effective.

package main

import (
    "fmt"
    "log"
    "runtime"
    "sync"
    "time"
)

const (
    Url = "http://128.32.37.201:8079/api/query"
    YearNS = 31536000000000000
    UuidDestination = "uuids.txt"
    MetadataDestination = "metadata1.txt"
    FileSize = 10000000 //amount of records in each timeseries file
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

func (adm *ADMManager) processUuids() {
    adm.uuids = adm.reader.readUuids(adm.url)
    adm.log.updateLogMetadata(UUIDS_FETCHED, WRITE_COMPLETE)
}

func (adm *ADMManager) processMetadata() {
    if adm.log.getLogMetadata(METADATA_WRITTEN) == WRITE_COMPLETE {
        fmt.Println("Writing metadata complete")
        return
    }

    dataChan := make(chan *MetadataTuple)
    var wg sync.WaitGroup
    wg.Add(2)

    fmt.Println(adm.uuids)

    adm.workers.acquire()
    go func() {
        defer wg.Done()
        defer adm.workers.release()
        fmt.Println("Starting to read metadata")
        adm.reader.readMetadata(adm.url, adm.uuids, dataChan)
    }()

    adm.workers.acquire()
    adm.openIO.acquire()
    go func() {
        defer wg.Done()
        defer adm.workers.release()
        defer adm.openIO.release()
        fmt.Println("Starting to write metadata")
        adm.writer.writeMetadata(MetadataDestination, dataChan)
    }()
    fmt.Println(adm.workers.count(), adm.openIO.count())
    wg.Wait()
    adm.log.updateLogMetadata(METADATA_WRITTEN, WRITE_COMPLETE)
}

func (adm *ADMManager) run() {
    var wg sync.WaitGroup
    wg.Add(1)

    adm.processUuids()
    go func() {
        defer wg.Done()
        adm.processMetadata()
    }()
    wg.Wait()
}

func main() {
    go func() {
        for {
            time.Sleep(2 * time.Second)
            log.Println(runtime.NumGoroutine())
        }
    }()
    adm := newADMManager(Url, 10, 10)
    adm.run()

    //testing
    // ids := []string{"11d93edc-9a0e-5896-8cbe-4888ba52dcbd", "12a4db87-67e1-5fc7-99fa-33b01e32c0b4"}
    // fmt.Println(adm.reader.readWindow(adm.url, "11d93edc-9a0e-5896-8cbe-4888ba52dcbd"))
    // r := newNetworkReader(adm.log)
    // var w []*Window
    // w = r.readWindowsBatched(adm.url, ids)
    // fmt.Println(w[0], "\n", w[1])
    // fmt.Println(adm.uuids)
}
