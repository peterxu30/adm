package main

import (
    "fmt"
    "log"
    "runtime"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
)

const (
    YEAR_NS = 31536000000000000
    CHANNEL_BUFFER_SIZE = 10
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
    config *AdmConfig
    chunkSize int64
    log             *Logger
    errorChan chan *ErrorLog
}

func newADMManager(config *AdmConfig) *ADMManager {
    logger := newLogger()

    mdata_dest :=getDirPath(config.MetadataDest)
    os.MkdirAll(mdata_dest, os.ModePerm)

    tsr_dest := getDirPath(config.TimeseriesDest)
    fmt.Println("TSR DEST:", tsr_dest, mdata_dest)
    os.MkdirAll(tsr_dest, os.ModePerm)

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
        config: config,
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

    dataChan := make(chan *MetadataTuple, CHANNEL_BUFFER_SIZE)
    errored := false
    dest := adm.getMetadataDest()

    adm.workers.acquire()
    adm.openIO.acquire()
    go func() {
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
    }()

    adm.workers.acquire()
    adm.openIO.acquire()
    go func(dest string) {
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

    }(dest())

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
                return adm.config.MetadataDest //placeholder
            case RM_GILES:
                return adm.config.MetadataDest
            default:
                return adm.config.MetadataDest //placeholder
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
    // windows := generateDummyWindows(adm.uuids[0:5], YEAR_NS)
    // log.Println("timeseries uuids:", adm.uuids[0:5])
    // log.Println(windows[4])

    //ACTUAL WINDOWS
    windows := adm.processWindows()
    if len(windows) == 0 {
        log.Println("processTimeseriesData: no windows generated. Attempting to proceed with dummy windows.")
        windows = append(windows, generateDummyWindows(adm.uuids, adm.chunkSize, YEAR_NS)...)
    }

    windows = append(windows, generateDummyWindow("final", adm.chunkSize, YEAR_NS)) //to ensure final timeslot is processed

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

                dataChan := make(chan *TimeseriesTuple, CHANNEL_BUFFER_SIZE)
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
    minFreeWorkers := adm.config.WorkerSize - adm.workers.count()
    minFreeOpenIO := adm.config.OpenIO - adm.openIO.count()
    minFreeResources := minFreeWorkers
    if minFreeOpenIO < minFreeWorkers {
        minFreeResources = minFreeOpenIO
    }

    //2. number of uuids / min free resources = number of uuids per routine
    length := len(adm.uuids)
    log.Println(adm.uuids)
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
        go func(start int, end int) {
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
            received := make(map[string]bool)
            for _, window := range windowSlice {
                if _, ok := received[window.Uuid]; !ok {
                    fmt.Println("start:", start, "i:", i, "end:", end, "len windowSlice:", len(windowSlice), "len uuids:", len(adm.uuids[start:end]))
                    windowChan <- window
                    fmt.Println("put window in chan. curr len:", len(windowChan), length)
                    received[window.Uuid] = true
                }
            }
        }(i, end)
    }
    wg.Wait()
    close(windowChan)

    for window := range windowChan {
        windows = append(windows, window)
    }

    fmt.Println("All windows added:", len(windows), len(adm.uuids))

    return windows
}

func (adm *ADMManager) getTimeseriesDest() func() string {
    fileCount := 0
    return func() string {
        switch adm.writeMode {
            case WM_FILE:
                splt := strings.Split(adm.config.TimeseriesDest, ".")
                dest := splt[0] + strconv.Itoa(fileCount) + splt[1]
                fileCount++
                return dest
            case WM_GILES:
                return adm.config.TimeseriesDest
            default:
                return adm.config.TimeseriesDest //placeholder        
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
    go func() {
        defer adm.workers.release()
        defer wg.Done()
        adm.processMetadata()
        log.Println("run: metadata finished")
    }()

    adm.workers.acquire()
    go func() {
        defer adm.workers.release()
        defer wg.Done()
        adm.processTimeseriesData()
        log.Println("run: timeseries finished")
    }()
    wg.Wait()
    close(adm.errorChan)
    log.Println("run: adm finished")
}

func main() {
    admConfig, err := newAdmConfig()
    if err != nil {
        if !fileExists(CONFIG_FILE) {
            createConfigFile()
            log.Println("No config file detected.\nNew config file created.")
        } else {
            log.Println("Bad config file.")
        }
        return
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
}
