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
    "os"
    "strconv"
    "sync"
    "time"
)

const (
    Url = "http://128.32.37.201:8079/api/query"
    YearNS = 31536000000000000
    TimeseriesFolder = "data/timeseries/"
    UuidDestination = "uuids.txt"
    MetadataDestination = "data/metadata.txt"
    TimeseriesDestination = "nil"
    WorkerSize = 40
    OpenIO = 15
    ReaderType = RM_GILES
    WriterType = WM_FILE
    ChannelBuffer = 10
)

type ReadMode uint8

type WriteMode uint8

const (
    RM_GILES ReadMode = iota + 1
    RM_FILE
)

const (
    WM_GILES WriteMode = iota + 1
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
    channelBufferSize int
    chunkSize int64
    log             *Logger
    errorChan chan *ErrorLog
}

func newADMManager(config *AdmConfig) *ADMManager {
    logger := newLogger()

    reader := configureReader(config.ReadMode)

    if reader == nil {
        log.Println("fatal: read mode unknown")
        return nil
    }

    writer := configureWriter(config.WriteMode)

    if writer == nil {
        log.Println("fatal: write mode unknown")
        return nil
    }

    return &ADMManager{
        url:      config.SourceUrl,
        readMode: config.ReadMode,
        writeMode: config.WriteMode,
        reader: reader,
        writer: writer,
        workers: newSema(config.WorkerSize),
        openIO: newSema(config.OpenIO),
        channelBufferSize: config.ChannelBufferSize,
        chunkSize: config.ChunkSize,
        log:      logger,
        errorChan: make(chan *ErrorLog, 100),
    }
}

func configureReader(mode ReadMode) Reader {
    switch mode {
        case RM_GILES:
            return newGilesReader()
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
        case WM_GILES:
            fmt.Println("network writer not yet developed")
            break
        case WM_FILE:
            err := os.MkdirAll(TimeseriesFolder, os.ModePerm)
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
    wg.Add(2)

    dataChan := make(chan *MetadataTuple, adm.channelBufferSize)
    errored := false
    dest := adm.getMetadataDest()

    adm.workers.acquire()
    adm.openIO.acquire()
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        defer adm.workers.release()
        defer adm.openIO.release()
        fmt.Println("processMetadata: Starting to read metadata")
        err := adm.reader.readMetadata(adm.url, adm.uuids, dataChan)

        if err != nil {
            log.Println(err)
            if !err.Fatal() {
                //write the bad uuids to text
                for _, uuid := range err.Failed() {
                    badUuid := uuid.(string)
                    errLog := newErrorLog(badUuid, METADATA_ERROR, 0, 0)
                    adm.errorChan <- errLog
                }
            }
            errored = true
        }
    }(&wg)

    adm.workers.acquire()
    adm.openIO.acquire()
    go func(dest string, wg *sync.WaitGroup) {
        defer wg.Done()
        defer adm.workers.release()
        defer adm.openIO.release()
        fmt.Println("processMetadata: Starting to write metadata")
        err := adm.writer.writeMetadata(dest, dataChan)

        badUuids := make(map[string]bool)
        if err != nil {
            log.Println(err)
            if !err.Fatal() {
                for _, uuid := range err.Failed() {
                    badUuid := uuid.(string)

                    badUuids[badUuid] = true

                    errLog := newErrorLog(badUuid, METADATA_ERROR, 0, 0)
                    adm.errorChan <- errLog
                }
            }
            errored = true
        }

        for _, uuid := range adm.uuids {
            if _, ok := badUuids[uuid]; !ok {
                adm.log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
            }
        }

    }(dest(), &wg)

    wg.Wait()

    if !errored {
        adm.log.updateLogMetadata(METADATA_WRITTEN, WRITE_COMPLETE)
    } else {
        log.Println("processMetadata: some uuids could not be proccessed")
    }
    log.Println("processMetadata: completed")
}

func (adm *ADMManager) getMetadataDest() func() string {
    return func() string {
        switch adm.readMode {
            case RM_FILE:
                return MetadataDestination //placeholder
            case RM_GILES:
                return MetadataDestination
            default:
                return MetadataDestination //placeholder
        }
    }
}

func (adm *ADMManager) processTimeseriesData() {
    if adm.log.getLogMetadata(TIMESERIES_WRITTEN) == WRITE_COMPLETE {
        fmt.Println("processTimeseriesData: Writing timeseries complete")
        return
    }

    fmt.Println("processTimeseriesData: About to start processing timeseries data")

    //DUMMY WINDOWS
    // windows := adm.generateDummyWindows(adm.uuids[0:5])
    // log.Println("timeseries uuids:", adm.uuids[0:5])
    // log.Println(windows[4])

    //ACTUAL WINDOWS
    windows := adm.processWindows()
    if len(windows) == 0 {
        log.Println("processTimeseriesData: no windows generated. Attempting to proceed with dummy windows.")
        windows = append(windows, adm.generateDummyWindows(adm.uuids[0:5])...)
    }

    windows = append(windows, adm.generateDummyWindow("final", adm.chunkSize)) //to ensure final timeslot is processed

    var wg sync.WaitGroup
    dest := adm.getTimeseriesDest()
    currentSize := int64(0)
    slotsToWrite := make([]*TimeSlot, 0)
    errored := false
    fmt.Println("processTimeseriesData: got windows")
    for _, window := range windows { //each window represents one uuid
        log.Println("window:", window.Uuid)
        if window == nil {
            log.Println("window nil")
            continue
        }

        var timeSlots []*TimeSlot
        timeSlots = window.getTimeSlots()
        if (len(timeSlots) == 0) {
            log.Println("processTimeseriesData: no slots for", window.Uuid)
            continue
        }

        for _, timeSlot := range timeSlots {
            empty := timeSlot.Count == 0
            if adm.log.getUuidTimeseriesStatus(timeSlot) == WRITE_COMPLETE {
                log.Println("processTimeseriesData: slot", timeSlot.Uuid, "already finished")
                continue
            }

            if empty {
                adm.log.updateUuidTimeseriesStatus(timeSlot, WRITE_COMPLETE)
                log.Println("processTimeseriesData: no data in slot", timeSlot.Uuid)
            }

            currentSize += timeSlot.Count
            log.Println("current size:", currentSize)
            if !empty && timeSlot.Uuid != "final" {
                log.Println(timeSlot.Uuid, timeSlot.StartTime, timeSlot.EndTime, "count:", timeSlot.Count)
                slotsToWrite = append(slotsToWrite, timeSlot)
            }

            //dummy slot will always trigger final FileSize check
            if (currentSize >= adm.chunkSize) && len(slotsToWrite) > 0 { 

                dataChan := make(chan *TimeseriesTuple, adm.channelBufferSize)
                wg.Add(2)

                adm.workers.acquire()
                adm.openIO.acquire()
                log.Println("timeseries read resources acquired")
                go func(slotsToWrite []*TimeSlot, dataChan chan *TimeseriesTuple, wg *sync.WaitGroup) {
                    defer wg.Done()
                    defer adm.workers.release()
                    defer adm.openIO.release()
                    log.Println("timeseries read starting. # slots:", len(slotsToWrite))
                    err := adm.reader.readTimeseriesData(adm.url, slotsToWrite, dataChan)
                    if err != nil {
                        log.Println(err)
                        if !err.Fatal() {
                            for _, slot := range err.Failed() {
                                badSlot := slot.(*TimeSlot)
                                errLog := newErrorLog(badSlot.Uuid, TIMESERIES_ERROR, badSlot.StartTime, badSlot.EndTime)
                                adm.errorChan <- errLog
                            }
                        }
                        errored = true
                    }
                    log.Println("timeseries read, resources released")
                }(slotsToWrite, dataChan, &wg)

                adm.workers.acquire()
                adm.openIO.acquire()
                log.Println("timeseries write resources acquired")
                go func(dest string, slotsToWrite []*TimeSlot, dataChan chan *TimeseriesTuple, wg *sync.WaitGroup) {
                    defer wg.Done()
                    defer adm.workers.release()
                    defer adm.openIO.release()
                    log.Println("timeseries write starting", dest)
                    err := adm.writer.writeTimeseriesData(dest, dataChan)

                    badSlots := make(map[*TimeSlot]bool)
                    if err != nil {
                        log.Println(err)
                        if !err.Fatal() {
                            for _, slot := range err.Failed() {
                                badSlot := slot.(*TimeSlot)

                                badSlots[badSlot] = true

                                errLog := newErrorLog(badSlot.Uuid, TIMESERIES_ERROR, badSlot.StartTime, badSlot.EndTime)
                                adm.errorChan <- errLog
                            }
                        }
                        errored = true
                    }

                    for _, slot := range slotsToWrite {
                        if _, ok := badSlots[slot]; !ok {
                            adm.log.updateUuidTimeseriesStatus(slot, WRITE_COMPLETE)
                        }
                    }

                    log.Println("timeseries written, resources released")
                }(dest(), slotsToWrite, dataChan, &wg)

                if !empty {
                    currentSize = timeSlot.Count
                    slotsToWrite = make([]*TimeSlot, 0)
                }
            }
        }
    }

    wg.Wait()
    if !errored {
        adm.log.updateLogMetadata(TIMESERIES_WRITTEN, WRITE_COMPLETE)
    }
}

func (adm *ADMManager) processWindows() []*Window {
    var windows []*Window
    //1. Find minimum number free resources from workers and openIO
    minFreeWorkers := WorkerSize - adm.workers.count()
    minFreeOpenIO := OpenIO - adm.openIO.count()
    minFreeResources := minFreeWorkers
    if minFreeOpenIO < minFreeWorkers {
        minFreeResources = minFreeOpenIO
    }

    //2. number of uuids / min free resources = number of uuids per routine
    length := len(adm.uuids[0:2])
    log.Println(adm.uuids[0:2])
    // windows = make([]*Window, len(adm.uuids))
    numUuidsPerRoutine := length / minFreeResources
    if numUuidsPerRoutine == 0 {
        numUuidsPerRoutine = length
    }
    //3. each routine runs adm.reader.readWindows on a fixed range
    var wg sync.WaitGroup

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

            var windowSlice []*Window
            
            fmt.Println("processWindows: Processing windows", start, end)
            windowSlice, err := adm.reader.readWindows(adm.url, adm.uuids[start:end])
            if err != nil {
                log.Println(err)
                if !err.Fatal() {
                    for _, uuid := range err.Failed() {
                        badUuid := uuid.(string)
                        errLog := newErrorLog(badUuid, WINDOW_ERROR, 0, 0)
                        adm.errorChan <- errLog
                    }
                }
            }

            if len(windowSlice) != (end - start) {
                log.Println("Window mismatch:", (end - start), "uuids produced", len(windowSlice), "windows")
            }

            fmt.Println("putting window in chan")
            // for i, window := range windowSlice {
            for i := 0; i < end - start; i++ {
                fmt.Println(i, end-start)
                window := windowSlice[i]
                fmt.Println("start:", start, "i:", i, "end:", end, "len windowSlice:", len(windowSlice), "len uuids:", len(adm.uuids[start:end]))
                windowChan <- window
                fmt.Println("put window in chan. curr len:", len(windowChan), length)
            }
        }(i, end, windows, &wg)

        fmt.Println("processWindows: Go routine created.", i, end)
    }
    wg.Wait()
    close(windowChan)
    for window := range windowChan {
        fmt.Println("appending windows")
        windows = append(windows, window)
    }
    fmt.Println("All windows added:", len(windows), len(adm.uuids))

    return windows
}

func (adm *ADMManager) generateDummyWindows(uuids []string) (windows []*Window) {
    for _, uuid := range uuids {
        windows = append(windows, adm.generateDummyWindow(uuid, adm.chunkSize))
    }
    return
}

func (adm *ADMManager) generateDummyWindow(uuid string, size int64) *Window {
    readings := make([][]float64, 1)
    readings[0] = []float64{0, float64(size), 0, 0}
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
                dest := TimeseriesFolder + "ts_" + strconv.Itoa(fileCount)
                fileCount++
                return dest
            case WM_GILES:
                return TimeseriesDestination //placeholder
            default:
                return TimeseriesDestination //placeholder        
        }
    }
}

func (adm *ADMManager) errorLogger(dest string, errorChan chan *ErrorLog) {
    os.Create(dest)
    f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0644)

    if err != nil {
        log.Println("errorLogger: could not log bad data sources err:", err)
    }

    for errorLog := range errorChan {
        f.Write([]byte(fmt.Sprint(errorLog.uuid, " ", errorLog.errorType, errorLog.startTime, errorLog.endTime, "\n")))
    }

    err = f.Close()
    if err != nil {
        log.Println("errorLogger: could not close file")
    }
}

func (adm *ADMManager) run() {
    adm.workers.acquire()
    go func() {
        defer adm.workers.release()
        adm.errorLogger("dev/data_src_error_log", adm.errorChan)
        log.Println("run: errors finished")
    }()

    adm.processUuids()

    var wg sync.WaitGroup
    wg.Add(2)

    adm.workers.acquire()
    go func(wg *sync.WaitGroup) {
        defer adm.workers.release()
        defer wg.Done()
        adm.processMetadata()
        log.Println("run: metadata finished")
    }(&wg)

    adm.workers.acquire()
    go func(wg *sync.WaitGroup) {
        defer adm.workers.release()
        defer wg.Done()
        adm.processTimeseriesData()
        log.Println("run: timeseries finished")
    }(&wg)
    wg.Wait()
    close(adm.errorChan)
    log.Println("run: adm finished")
}

func main() {
    admConfig, err := newAdmConfig() //TODO: finish config
    if err != nil {
        log.Println("Bad config file.")
        // createConfigFile()
    }

    adm := newADMManager(admConfig)
    go func() {
        for {
            time.Sleep(10 * time.Second)
            log.Println("Number of go routines:", runtime.NumGoroutine())
            log.Println("Number of workers:", adm.workers.count(), "Number of open IO:", adm.openIO.count())
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
