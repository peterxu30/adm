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
    // "runtime"
    "os"
    "strconv"
    "sync"
    "time"
)

const (
    Url = "http://128.32.37.201:8079/api/query"
    YearNS = 31536000000000000
    DataFolder = "data/"
    TimeseriesFolder = "timeseries/"
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
    readMode ReadMode
    writeMode WriteMode
    reader Reader
    writer Writer
    workers *Sema
    openIO *Sema
    log             *Logger
}

func newADMManager(url string, workerSize int, openIO int, readMode ReadMode, writeMode WriteMode) *ADMManager {
    logger := newLogger()

    reader := configureReader(readMode)

    if reader == nil {
        log.Println("fatal: read mode unknown")
        return nil
    }

    writer := configureWriter(writeMode)

    if writer == nil {
        log.Println("fatal: write mode unknown")
        return nil
    }

    return &ADMManager{
        url:      url,
        readMode: readMode,
        writeMode: writeMode,
        reader: reader,
        writer: writer,
        workers: newSema(workerSize),
        openIO: newSema(openIO),
        log:      logger,
    }
}

func configureReader(mode ReadMode) Reader {
    switch mode {
        case RM_NETWORK:
            return newNetworkReader()
        case RM_FILE:
            fmt.Println("file reader not yet developed")
            return nil
        default:
            return nil
    }
    return nil
}

func configureWriter(mode WriteMode) Writer {
    switch mode {
        case WM_NETWORK:
            fmt.Println("network writer not yet developed")
            break
        case WM_FILE:
            err := os.MkdirAll(DataFolder + TimeseriesFolder, os.ModePerm)
            if err != nil {
                log.Println("configureWriter: could not create data folder")
                return nil
            }
            return newFileWriter()
        default:
            log.Println("write mode unknown")
            return nil
    }
    return nil
}

func (adm *ADMManager) processUuids() {
    if (adm.log.getLogMetadata(UUIDS_FETCHED) == WRITE_COMPLETE) {
        fmt.Println("processUuids: uuids were previously read")
        adm.uuids = adm.log.getUuidMetadataKeySet()
        return
    }

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

func (adm *ADMManager) processMetadata() {
    if adm.log.getLogMetadata(METADATA_WRITTEN) == WRITE_COMPLETE {
        fmt.Println("processMetadata: Writing metadata complete")
        return
    }

    var wg sync.WaitGroup
    wg.Add(1)

    adm.workers.acquire()
    go func(wg *sync.WaitGroup) { //TODO: UNTESTED
        defer adm.workers.release()
        defer wg.Done()
        finished := false
        errored := false
        attempts := 0
        dest := adm.getMetadataDest()
        for !finished && attempts < MaxTries {
            var innerWg sync.WaitGroup
            innerWg.Add(2)
            dataChan := make(chan *MetadataTuple)

            adm.workers.acquire()
            adm.openIO.acquire()
            go func(wg *sync.WaitGroup) {
                defer adm.workers.release()
                defer adm.openIO.release()
                defer innerWg.Done()
                fmt.Println("processMetadata: Starting to read metadata")
                err := adm.reader.readMetadata(adm.url, adm.uuids, dataChan) //TODO: Potentially bad style to not pass variables into routine.
                if err != nil {
                    log.Println(err)
                    errored = true
                }
            }(&innerWg)

            adm.workers.acquire()
            adm.openIO.acquire()
            go func(wg *sync.WaitGroup) {
                defer adm.workers.release()
                defer adm.openIO.release()
                defer wg.Done()
                fmt.Println("processMetadata: Starting to write metadata")
                err := adm.writer.writeMetadata(dest(), dataChan)
                if err != nil {
                    log.Println(err)
                    errored = true
                }
            }(&innerWg)

            innerWg.Wait()

            finished = !errored
            if finished {
                for _, uuid := range adm.uuids {
                    adm.log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
                }
            } else { //incase parallelization of metadata processing is ever needed
                time.Sleep(time.Duration(TryAgainInterval * attempts) * time.Second)
            }
            attempts++
        }

        if finished {
            adm.log.updateLogMetadata(METADATA_WRITTEN, WRITE_COMPLETE)
        } else {
            log.Println("processMetadata: maximum attempts exceeded")
        }
    }(&wg)

    wg.Wait()
}

func (adm *ADMManager) getMetadataDest() func() string {
    return func() string {
        switch adm.readMode {
            case RM_FILE:
                return DataFolder + MetadataDestination //placeholder
            case RM_NETWORK:
                return DataFolder + MetadataDestination
            default:
                return DataFolder + MetadataDestination //placeholder
        }
    }
}

func (adm *ADMManager) processTimeseriesData() {
    if adm.log.getLogMetadata(TIMESERIES_WRITTEN) == WRITE_COMPLETE {
        fmt.Println("processTimeseriesData: Writing timeseries complete")
        return
    }

    fmt.Println("processTimeseriesData: About to start processing timeseries data")

    windows, err := adm.processWindows()
    if err != nil {
        log.Println("processTimeseriesData: unable to retrieve windows, processing timeseries data cannot continue")
        return
    }

    var wg sync.WaitGroup
    dest := adm.getTimeseriesDest()
    currentSize := 0
    slotsToWrite := make([]*TimeSlot, 0)
    anyError := false
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
                wg.Add(1)

                adm.workers.acquire()
                go func(dest string, slotsToWrite []*TimeSlot, wg *sync.WaitGroup) {
                    defer adm.workers.release()
                    defer wg.Done()

                    finished := false
                    errored := false
                    attempts := 0
                    for !finished && attempts < MaxTries {
                        var innerWg sync.WaitGroup
                        innerWg.Add(2)
                        dataChan := make(chan *TimeseriesTuple)

                        adm.workers.acquire()
                        adm.openIO.acquire()
                        go func(slotsToWrite []*TimeSlot, dataChan chan *TimeseriesTuple, wg *sync.WaitGroup) {
                            defer adm.workers.release()
                            defer adm.openIO.release()
                            defer wg.Done()
                            err := adm.reader.readTimeseriesData(adm.url, slotsToWrite, dataChan)
                            if err != nil {
                                log.Println(err)
                                errored = true
                            }
                        }(slotsToWrite, dataChan, &innerWg)

                        adm.workers.acquire()
                        adm.openIO.acquire()
                        go func(dest string, dataChan chan *TimeseriesTuple, wg *sync.WaitGroup) {
                            defer adm.workers.release()
                            defer adm.openIO.release()
                            defer wg.Done()
                            err := adm.writer.writeTimeseriesData(dest, dataChan)
                            if err != nil {
                                log.Println(err)
                                errored = true
                            }
                        }(dest, dataChan, &innerWg)

                        innerWg.Wait()

                        finished = !errored
                        if finished {
                            for _, slot := range slotsToWrite {
                                adm.log.updateUuidTimeseriesStatus(slot, WRITE_COMPLETE)
                            }
                        } else {
                            time.Sleep(time.Duration(TryAgainInterval * attempts) * time.Second)
                        }
                        attempts++
                    }

                    if !finished {
                        log.Println("processTimeseriesData: maximum attempts exceeded for batch")
                        os.Remove(dest)
                        anyError = true
                    }
                }(dest(), slotsToWrite, &wg)

                currentSize = timeSlot.Count
                slotsToWrite = make([]*TimeSlot, 0)
            }
            slotsToWrite = append(slotsToWrite, timeSlot)
        }
    }

    wg.Wait()
    if !anyError {
        adm.log.updateLogMetadata(TIMESERIES_WRITTEN, WRITE_COMPLETE)
    }
}

func (adm *ADMManager) processWindows() (windows []*Window, err error) {
    //1. Find minimum number free resources from workers and openIO
    minFreeWorkers := WorkerSize - adm.workers.count()
    minFreeOpenIO := OpenIO - adm.openIO.count()
    minFreeResources := minFreeWorkers
    if minFreeOpenIO < minFreeWorkers {
        minFreeResources = minFreeOpenIO
    }

    //2. number of uuids / min free resources = number of uuids per routine
    length := len(adm.uuids)
    // windows = make([]*Window, len(adm.uuids))
    numUuidsPerRoutine := len(adm.uuids) / minFreeResources
    //3. each routine runs adm.reader.readWindows on a fixed range
    var wg sync.WaitGroup
    errored := false

    //test
    windowChan := make(chan *Window, length)
    for i := 0; i < length; i += numUuidsPerRoutine {
        end := i + numUuidsPerRoutine

        if end > length {
            end = length
        }

        adm.workers.acquire()
        adm.openIO.acquire()
        wg.Add(1)
        go func(start int, end int, windows []*Window, wg *sync.WaitGroup) {
            defer adm.workers.release()
            defer adm.openIO.release()
            defer wg.Done()

            finished := false
            attempts := 0
            var windowSlice []*Window
            for !finished && attempts < MaxTries {
                fmt.Println("processWindows: Processing windows", start, end)
                windowSlice, err = adm.reader.readWindows(adm.url, adm.uuids[start:end])
                if err != nil {
                    log.Println(err)
                } else {
                    finished = true
                }
                attempts++
            }

            if finished {
                // fmt.Println("processWindows:", start, end, adm.uuids[start:end], windowSlice)
                fmt.Println("putting window in chan")
                // for i, window := range windowSlice {
                for i := 0; i < end - start; i++ {
                    window := windowSlice[i]
                    fmt.Println("start:", start, "i:", i, "end:", end, "len windowSlice:", len(windowSlice), "len uuids:", len(adm.uuids[start:end]))
                    // windows[start + i] = window
                    fmt.Println("putting window in chan", start, end, len(windowSlice))
                    windowChan <- window
                    fmt.Println("put window in chan. curr len:", len(windowChan), length)
                }
                // fmt.Println("putting window in chan")
                // fmt.Println("size of windowSlice before this write:", len(windowSlice), len(windowSliceChan), len(adm.uuids))
                // windowSliceChan <- windowSlice //TODO: Blocks for some reason
                fmt.Println("put windows in chan")
            } else {
                errored = true
            }
        }(i, end, windows, &wg)

        fmt.Println("processWindows: Go routine created.", i, end)
    }
    fmt.Println("loop finished")
    wg.Wait()
    close(windowChan)
    for window := range windowChan {
        fmt.Println("appending windows")
        windows = append(windows, window)
    }
    fmt.Println("All windows added:", len(windows), len(adm.uuids))

    fmt.Println("processWindows:", windows, "WINDOWS FINISHED")

    if errored {
        return nil, fmt.Errorf("processWindows: failed to process windows")
    }
    return windows, nil
}

func (adm *ADMManager) generateDummyWindows(uuids []string) (windows []*Window) {
    for _, uuid := range uuids {
        windows = append(windows, adm.generateDummyWindow(uuid))
    }
    return
}

func (adm *ADMManager) generateDummyWindow(uuid string) *Window {
    readings := make([][]float64, 1)
    readings[0] = []float64{0, FileSize, 0, 0}
    return &Window {
        Uuid: uuid,
        Readings: readings,
    }
}

func (adm *ADMManager) getTimeseriesDest() func() string {
    fileCount := 0
    return func() string {
        switch adm.writeMode {
            case WM_FILE:
                dest := DataFolder + TimeseriesFolder + "ts_" + strconv.Itoa(fileCount)
                fileCount++
                return dest
            case WM_NETWORK:
                return TimeseriesDestination //placeholder
            default:
                return TimeseriesDestination //placeholder        
        }
    }
}

func (adm *ADMManager) run() {
    adm.processUuids()

    finished := false
    attempts := 0
    for !finished && attempts < MaxTries { //TODO: revisit this methodology

        var wg sync.WaitGroup
        wg.Add(2)

        adm.workers.acquire()
        go func(wg *sync.WaitGroup) {
            defer adm.workers.release()
            defer wg.Done()
            adm.processMetadata()
        }(&wg)

        adm.workers.acquire()
        go func(wg *sync.WaitGroup) {
            defer adm.workers.release()
            defer wg.Done()
            adm.processTimeseriesData()
        }(&wg)
        fmt.Println("run: Waiting")
        wg.Wait()

        attempts++
        finished = adm.checkIfMetadataProcessed() && adm.checkIfTimeseriesProcessed()
    }

    if !finished {
        log.Println("adm was unable to complete the job")
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
    adm := newADMManager(Url, WorkerSize, OpenIO, RM_NETWORK, WM_FILE)
    go func() {
        for {
            time.Sleep(2 * time.Second)
            // log.Println("Number of go routines:", runtime.NumGoroutine())
            // log.Println("Number of workers:", adm.workers.count(), "Number of open IO:", adm.openIO.count())
        }
    }()

    os.Mkdir("dev/", os.ModePerm)
    outFile, err := os.Create("dev/stdout")
    if err == nil {
        os.Stdout = outFile
    }
    fmt.Println("Stdout initialized.")

    logFile, err := os.Create("dev/log")
    if err == nil {
        log.SetOutput(logFile)
    }
    log.Println("Log initialized.")

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
