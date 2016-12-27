//TODO: At the moment, the log is checked at every level to see if a job was done or not. After adm methods are better flushed out, go back and consider where logging is most effective.
//Thought: Right now, uuids and window queries are written to the log for on disk retrieval in event of crash. Writing to log slows down program in exchange for betetr crash recovery performance.
//However, crash recovery is not what should be optimized. Inexpensive normal performance and expensive crash recovery is ideal. Revisit this.
//Make a function that encloses all the parallelization logic. Acquiring/releasing resources etc.

package main

import (
    "fmt"
    "log"
    "runtime"
    "strconv"
    "sync"
    "time"
)

const (
    Url = "http://128.32.37.201:8079/api/query"
    YearNS = 31536000000000000
    UuidDestination = "uuids.txt"
    MetadataDestination = "metadata1.txt"
    FileSize = 10000000 //amount of records in each timeseries file
    WorkerSize = 40
    OpenIO = 15
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

    adm.workers.acquire()
    adm.openIO.acquire()
    go func() {
        defer adm.workers.release()
        defer adm.openIO.release()
        defer wg.Done()
        fmt.Println("Starting to read metadata")
        adm.reader.readMetadata(adm.url, adm.uuids, dataChan)
    }()

    adm.workers.acquire()
    adm.openIO.acquire()
    go func() {
        defer adm.workers.release()
        defer adm.openIO.release()
        defer wg.Done()
        fmt.Println("Starting to write metadata")
        adm.writer.writeMetadata(MetadataDestination, dataChan)
    }()

    wg.Wait()
    adm.log.updateLogMetadata(METADATA_WRITTEN, WRITE_COMPLETE)
}

func (adm *ADMManager) processTimeseriesData() {
    if adm.log.getLogMetadata(TIMESERIES_WRITTEN) == WRITE_COMPLETE {
        fmt.Println("Writing timeseries complete")
        return
    }

    fmt.Println("About to start processing timeseries data")

    windows := adm.reader.readWindows(adm.url, adm.uuids)

    var wg sync.WaitGroup
    fileCount := 0
    currentSize := 0
    slotsToWrite := make([]*TimeSlot, 0)
    fmt.Println("got windows")
    for _, window := range windows { //each window represents one uuid

        if window == nil {
            continue
        }

        var timeSlots []*TimeSlot
        timeSlots = window.getTimeSlots()
        if (len(timeSlots) == 0) {
            continue
        }

        for _, timeSlot := range timeSlots {
            if adm.log.getUuidTimeseriesStatus(timeSlot) == WRITE_COMPLETE {
                continue
            } else if timeSlot.Count == 0 {
                adm.log.updateUuidTimeseriesStatus(timeSlot, WRITE_COMPLETE)
                continue
            }

            currentSize += timeSlot.Count

            if currentSize >= FileSize { //convert to memory size check later
                fileName := "ts" + strconv.Itoa(fileCount)
                wg.Add(2)
                dataChan := make(chan *TimeseriesTuple)

                adm.workers.acquire()
                adm.openIO.acquire()
                go func(slotsToWrite []*TimeSlot) {
                    defer adm.workers.release()
                    defer adm.openIO.release()
                    defer wg.Done()
                    adm.reader.readTimeseriesData(adm.url, slotsToWrite, dataChan)
                }(slotsToWrite)

                adm.workers.acquire()
                adm.openIO.acquire()
                go func(fileName string) {
                    defer adm.workers.release()
                    defer adm.openIO.release()
                    defer wg.Done()
                    adm.writer.writeTimeseriesData(fileName, dataChan) //TODO: dest is not always a file name. Depends on the writer type. Write a function that returns dest based on writer type.
                }(fileName)

                currentSize = timeSlot.Count
                fileCount++
                slotsToWrite = make([]*TimeSlot, 0)
            }
            slotsToWrite = append(slotsToWrite, timeSlot)
        }
    }

    wg.Wait()
    adm.log.updateLogMetadata(TIMESERIES_WRITTEN, WRITE_COMPLETE)
}

func (adm *ADMManager) run() {
    adm.processUuids()

    finished := false
    for !finished {

        var wg sync.WaitGroup
        wg.Add(2)

        adm.workers.acquire()
        go func() {
            defer adm.workers.release()
            defer wg.Done()
            adm.processMetadata()
        }()

        adm.workers.acquire()
        go func() {
            defer adm.workers.release()
            defer wg.Done()
            adm.processTimeseriesData()
        }()
        fmt.Println("Waiting")
        wg.Wait()

        finished = adm.checkIfMetadataProcessed() && adm.checkIfTimeseriesProcessed()
    }
}

func (adm *ADMManager) checkIfMetadataProcessed() bool {
    keySet := adm.log.getUuidMetadataKeySet()
    for _, key := range keySet {
        if adm.log.getUuidMetadataStatus(key) != WRITE_COMPLETE {
            return false
        }
    }
    return true
}

func (adm *ADMManager) checkIfTimeseriesProcessed() bool {
    keySet := adm.log.getUuidTimeseriesKeySet()
    for _, key := range keySet {
        if adm.log.getUuidTimeseriesStatus(key) != WRITE_COMPLETE {
            return false
        }
    }
    return true
}

func main() {
    adm := newADMManager(Url, WorkerSize, OpenIO)
    go func() {
        for {
            time.Sleep(2 * time.Second)
            log.Println("Number of go routines:", runtime.NumGoroutine())
            log.Println("Number of workers:", adm.workers.count(), "Number of open IO:", adm.openIO.count())
        }
    }()
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
