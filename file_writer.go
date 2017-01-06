package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	// "log"
	"os"
)


type FileWriter struct {
	log *Logger
}

func newFileWriter(log *Logger) *FileWriter {
	return &FileWriter{
		log: log,
	}
}

func (w *FileWriter) writeUuids(dest string, uuids []string) (err error) {
	if w.log.getLogMetadata(UUIDS_WRITTEN) == WRITE_COMPLETE {
		return
	}

	body, err := json.Marshal(uuids)
	if err != nil {
		// log.Println("writeUuids: could not marshal uuids \n reason:", err)
		err = fmt.Errorf("writeUuids: could not marshal uuids:", uuids, "err:", err) //TODO: HANDLE ERRORS LIKE THIS
		return
	}

	err = ioutil.WriteFile(dest, body, 0644)
	if err != nil {
		// log.Println("writeUuids: could not write uuids. err:", err)
		err = fmt.Errorf("writeUuids: could not write uuids:", uuids, "err:", err)
		return
	}

	return
}

func (w *FileWriter) writeMetadata(dest string, dataChan chan *MetadataTuple) (err error) {
	if w.log.getLogMetadata(METADATA_WRITTEN) == WRITE_COMPLETE {
		return
	}

	if !w.fileExists(dest) {
		err := ioutil.WriteFile(dest, []byte("["), 0644)
	    if err != nil {
	    	// log.Println("writeMetadata: could not create metadata file. err:", err)
	    	return fmt.Errorf("writeMetadata: could not create metadata file:", dest, "err:", err)
	    }
	}

	f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		// log.Println("writeMetadata: could not open metadata file. err:", err)
		return fmt.Errorf("writeMetadata: could not open metadata file:", dest, "err:", err)
	}

	first := true
	wrote := false
	for tuple := range dataChan {
		for _, uuid := range tuple.uuids {
			if w.log.getUuidMetadataStatus(uuid) != WRITE_COMPLETE {
				break
			} else {
				continue
			}
		}

		if !first {
			_, err := f.Write([]byte(","))
			if err != nil {
				return fmt.Errorf("writeMetadata: could not write metadata file:", dest, "err:", err)
			}
		} else {
			first = false
		}

		_, err := f.Write(tuple.data)
		if err != nil {
			return fmt.Errorf("writeMetadata: could not write uuids:", tuple.uuids, "to metadata file:", dest, "err:", err)
		}
		wrote = true

		for _, uuid := range tuple.uuids {
			w.log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
		}
	}

	if wrote {
		_, err = f.Write([]byte("]"))
		if err != nil {
			return fmt.Errorf("writeMetadata: could not write metadata file:", dest, "err:", err)
		}
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("writeMetadata: could not close metadata file:", dest, "err:", err)
	}

	return
}

func (w *FileWriter) writeTimeseriesData(dest string, dataChan chan *TimeseriesTuple) (err error) {
	if w.log.getLogMetadata(TIMESERIES_WRITTEN) == WRITE_COMPLETE {
		return
	}

	if !w.fileExists(dest) {
		err := ioutil.WriteFile(dest, []byte("["), 0644)
	    if err != nil {
			return fmt.Errorf("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err)
	    }
	}

	f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err)
	}

	first := true
	wrote := false
	for tuple := range dataChan {
		if w.log.getUuidTimeseriesStatus(tuple.slot) == WRITE_COMPLETE {
			continue
		}

		if !first {
			_, err := f.Write([]byte(","))
			if err != nil {
				return fmt.Errorf("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err)
			}
		} else {
			first = false
		}

		_, err := f.Write(tuple.data)
		if err != nil {
			return fmt.Errorf("writeTimeseriesData: could not write slot:", tuple.slot, "to timeseries data file:", dest, "err:", err)
		}
		wrote = true
		w.log.updateUuidTimeseriesStatus(tuple.slot, WRITE_COMPLETE)
	}

	if wrote {
		_, err = f.Write([]byte("]"))
		if err != nil {
			return fmt.Errorf("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err)
		}
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("writeMetadata: could not close metadata file:", dest, "err:", err)
	}

	return
}

/*
cite: http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
*/
func (w *FileWriter) fileExists(file string) bool {
	if _, err := os.Stat(file); err == nil {
		return true
	}
	return false
}
