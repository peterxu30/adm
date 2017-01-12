//TODO: At the moment, the log is checked at every level to see if a job was done or not. After adm methods are better flushed out, go back and consider where logging is most effective.
//TODO: Error handling. Switch all (non-debugging) fmt.Println with log.Println. Don't panic unless its unrecoverable.
//Thought: Right now, uuids and window queries are written to the log for on disk retrieval in event of crash. Writing to log slows down program in exchange for better crash recovery performance.
//However, crash recovery is not what should be optimized. Inexpensive normal performance and expensive crash recovery is ideal. Revisit this.
//Remove logging from reader/writer classes. Logging should only record successful batch operations, not partial success/failures.
//Handle logging solely at the adm level. Remove window logging and/or handle it at adm level. Same with uuids
//Move all written files to a folder. Sub-folder for timeseries. When reading from files, use filepath.Walk. Name of files doesn't matter. 

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
    TimeseriesDestination = "nil"
    FileSize = 10000000 //amount of records in each timeseries file
    WorkerSize = 40
    OpenIO = 15
    MaxTries = 3
    TryAgainInterval = 3
    ReaderType = RM_NETWORK
    WriterType = WM_FILE
)

type ReadMode uint8

type WriteMode uint8

const (
    RM_NETWORK ReadMode = iota + 1
    RM_FILE
)

const (
    WM_NETWORK WriteMode = iota + 1
    WM_FILE
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
    uuids, err := adm.reader.readUuids(adm.url)
    if err == nil {
        adm.uuids = uuids
        for _, uuid := range adm.uuids {
            adm.log.updateUuidMetadataStatus(uuid, NOT_STARTED)
        }
        adm.log.updateLogMetadata(UUIDS_FETCHED, WRITE_COMPLETE)
    } else {
        log.Println(err)
    }
}

func (adm *ADMManager) processMetadata() (err error) {
    if adm.log.getLogMetadata(METADATA_WRITTEN) == WRITE_COMPLETE {
        fmt.Println("processMetadata: Writing metadata complete")
        return
    }

    var wg sync.WaitGroup
    wg.Add(1)

    returnChannel := make(chan error)

    adm.workers.acquire()
    go func() { //TODO: UNTESTED
        defer adm.workers.release()
        defer wg.Done()
        finished := false
        errored := false
        failedAttempts := 0
        for !finished && failedAttempts < MaxTries {
            var innerWg sync.WaitGroup
            innerWg.Add(2)
            dataChan := make(chan *MetadataTuple)

            adm.workers.acquire()
            adm.openIO.acquire()
            go func() {
                defer adm.workers.release()
                defer adm.openIO.release()
                defer innerWg.Done()
                fmt.Println("processMetadata: Starting to read metadata")
                err := adm.reader.readMetadata(adm.url, adm.uuids, dataChan) //TODO: Potentially bad style to not pass variables into routine.
                if err != nil {
                    log.Println(err)
                    errored = true
                }
            }()

            adm.workers.acquire()
            adm.openIO.acquire()
            go func() {
                defer adm.workers.release()
                defer adm.openIO.release()
                defer innerWg.Done()
                fmt.Println("processMetadata: Starting to write metadata")
                err := adm.writer.writeMetadata(MetadataDestination, dataChan)
                if err != nil {
                    log.Println(err)
                    errored = true
                }
            }()

            innerWg.Wait()

            finished = !errored
            if !finished {
                failedAttempts++
                time.Sleep(time.Duration(TryAgainInterval * failedAttempts) * time.Second)
            }
        }

        if finished {
            adm.log.updateLogMetadata(METADATA_WRITTEN, WRITE_COMPLETE)
            returnChannel <- nil
        } else {
            returnChannel <- fmt.Errorf("processMetadata: maximum attempts exceeded")
        }
        close(returnChannel)
    }()

    wg.Wait()
    return <- returnChannel
}

func (adm *ADMManager) processTimeseriesData() {
    if adm.log.getLogMetadata(TIMESERIES_WRITTEN) == WRITE_COMPLETE {
        fmt.Println("processTimeseriesData: Writing timeseries complete")
        return
    }

    fmt.Println("processTimeseriesData: About to start processing timeseries data")

    windows := adm.processWindows()

    var wg sync.WaitGroup
    dest := adm.getTimeseriesDest()
    currentSize := 0
    slotsToWrite := make([]*TimeSlot, 0)
    errored := false
    fmt.Println("processTimeseriesData: got windows")
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
                wg.Add(2)
                dataChan := make(chan *TimeseriesTuple)

                //wrap in go function that checks for errors from either operation and repeats them if so with increasing periods of time
                adm.workers.acquire()
                adm.openIO.acquire()
                go func(slotsToWrite []*TimeSlot, dataChan chan *TimeseriesTuple) {
                    defer adm.workers.release()
                    defer adm.openIO.release()
                    defer wg.Done()
                    adm.reader.readTimeseriesData(adm.url, slotsToWrite, dataChan)
                }(slotsToWrite, dataChan)

                adm.workers.acquire()
                adm.openIO.acquire()
                go func(dest string, dataChan chan *TimeseriesTuple) {
                    defer adm.workers.release()
                    defer adm.openIO.release()
                    defer wg.Done()
                    err := adm.writer.writeTimeseriesData(dest, dataChan)
                    if err != nil { //TODO: better error handling
                        log.Println(err)
                        errored = true
                    }
                }(dest(), dataChan)

                currentSize = timeSlot.Count
                slotsToWrite = make([]*TimeSlot, 0)
            }
            slotsToWrite = append(slotsToWrite, timeSlot)
        }
    }

    wg.Wait()
    if !errored {
        adm.log.updateLogMetadata(TIMESERIES_WRITTEN, WRITE_COMPLETE)
    }
}

//TODO: Function untested.
func (adm *ADMManager) processWindows() []*Window {
    //1. Find minimum number free resources from workers and openIO
    minFreeWorkers := WorkerSize - adm.workers.count()
    minFreeOpenIO := OpenIO - adm.openIO.count()
    minFreeResources := minFreeWorkers
    if minFreeOpenIO < minFreeWorkers {
        minFreeResources = minFreeOpenIO
    }

    //2. number of uuids / min free resources = number of uuids per routine
    length := len(adm.uuids)
    numUuidsPerRoutine := len(adm.uuids) / minFreeResources
    //3. each routine runs adm.reader.readWindows on a fixed range
    windows := make([]*Window, length)
    var wg sync.WaitGroup
    for i := 0; i < length; i += numUuidsPerRoutine {
        end := i + numUuidsPerRoutine

        if end > length {
            end = length
        }

        adm.workers.acquire()
        adm.openIO.acquire()
        wg.Add(1)
        go func(start int, end int, windows []*Window) {
            defer adm.workers.release()
            defer adm.openIO.release()
            defer wg.Done()
            fmt.Println("processWindows: Processing windows", start, end)
            windowSlice, err := adm.reader.readWindows(adm.url, adm.uuids[start:end])
            if err != nil {
                log.Println(err)
                return
            }
            fmt.Println("processWindows:", start, end, adm.uuids[start:end], windowSlice)
            for i, window := range windowSlice {
                fmt.Println(start, i)
                windows[start + i] = window
            }
        }(i, end, windows)
        fmt.Println("processWindows: Go routine created.", i, end)
    }
    fmt.Println("processWindows:", windows, "WINDOWS FINISHED")
    wg.Wait()
    return windows
}

func (adm *ADMManager) getTimeseriesDest() func() string {
    fileCount := 0

    return func() string {
        if WriterType == WM_FILE {
            dest := "ts_" + strconv.Itoa(fileCount)
            fileCount++
            return dest
        } else {
            return TimeseriesDestination          
        }
    }
}

func (adm *ADMManager) run() {
    adm.processUuids()

    finished := false
    for !finished { //TODO: revisit this methodology

        var wg sync.WaitGroup
        wg.Add(2)

        adm.workers.acquire()
        go func() {
            defer adm.workers.release()
            defer wg.Done()
            err := adm.processMetadata()
            if err != nil {
                log.Println(err)
            }
        }()

        adm.workers.acquire()
        go func() {
            defer adm.workers.release()
            defer wg.Done()
            adm.processTimeseriesData()
        }()
        fmt.Println("run: Waiting")
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
