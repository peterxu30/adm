//deletes the file on a fatal write error

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)


type FileWriter struct{}

func newFileWriter() *FileWriter {
	return &FileWriter{}
}

func (w *FileWriter) writeUuids(dest string, uuids []string) *ProcessError {
	body, err := json.Marshal(uuids)
	if err != nil {
		return newProcessError(fmt.Sprint("writeUuids: could not marshal uuids:", uuids, "err:", err), true, nil)
	}

	err = ioutil.WriteFile(dest, body, 0644)
	if err != nil {
		os.Remove(dest)
		return newProcessError(fmt.Sprint("writeUuids: could not write uuids:", uuids, "err:", err), true, nil)
	}

	return nil
}

func (w *FileWriter) writeMetadata(dest string, dataChan chan *MetadataTuple) *ProcessError {
	if !fileExists(dest) {
		err := ioutil.WriteFile(dest, []byte("["), 0644)
	    if err != nil {
	    	return newProcessError(fmt.Sprint("writeMetadata: could not create metadata file:", dest, "err:", err), true, nil)
	    }
	}

	f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		os.Remove(dest)
		return newProcessError(fmt.Sprint("writeMetadata: could not open metadata file:", dest, "err:", err), true, nil)
	}

	first := true
	wrote := false
	failed := make([]interface{}, 0)
	for tuple := range dataChan {
		if !first {
			_, err := f.Write([]byte(","))
			if err != nil {
				os.Remove(dest)
				return newProcessError(fmt.Sprint("writeMetadata: failed to write err:", err), true, nil)
			}
		} else {
			first = false
		}

		_, err := f.Write(tuple.data)
		if err != nil {
			log.Println("writeMetadata: failed to write uuid:", tuple.uuids)
			for _, uuid := range tuple.uuids {
				failed = append(failed, uuid)
			}
		}
		wrote = true
	}

	if wrote {
		_, err = f.Write([]byte("]"))
		if err != nil {
			os.Remove(dest)
			return newProcessError(fmt.Sprint("writeMetadata: could not write metadata file:", dest, "err:", err), true, nil)
		}
	}

	err = f.Close()
	if err != nil {
		os.Remove(dest)
		return newProcessError(fmt.Sprint("writeMetadata: could not close metadata file:", dest, "err:", err), true, nil)
	}

	if len(failed) > 0 {
		return newProcessError(fmt.Sprint("writeMetadata: failed to write uuids:", failed), false, failed)
	}
	return nil
}

func (w *FileWriter) writeTimeseriesData(dest string, dataChan chan *TimeseriesTuple) *ProcessError {
	if !fileExists(dest) {
		err := ioutil.WriteFile(dest, []byte("["), 0644)
	    if err != nil {
			return newProcessError(fmt.Sprint("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err), true, nil)
	    }
	}

	f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		os.Remove(dest)
		return newProcessError(fmt.Sprint("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err), true, nil)
	}

	first := true
	wrote := false
	failed := make([]interface{}, 0)
	for tuple := range dataChan {
		log.Println("writeTimeseriesData: write start for uuid", tuple.slot.Uuid, tuple.slot.StartTime, tuple.slot.EndTime, tuple.slot.Count, "to dest", dest)
		// if !first {
		// 	_, err := f.Write([]byte(","))
		// 	if err != nil {
		// 		os.Remove(dest)
		// 		return newProcessError(fmt.Sprint("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err), true, nil)
		// 	}
		// } else {
		// 	first = false
		// }

		data := tuple.data
		if !first {
			comma := []byte(",")
			data = append(comma, data...)
		} else {
			first = false
		}

		_, err := f.Write(data)
		if err != nil {
			fmt.Println("writeTimeseriesData: could not write slot:", tuple.slot, tuple.slot.StartTime, tuple.slot.EndTime, "to timeseries data file:", dest, "err:", err)
			failed = append(failed, tuple.slot)
		}
		wrote = true
		log.Println("writeTimeseriesData: write complete for uuid", tuple.slot.Uuid, tuple.slot.StartTime, tuple.slot.EndTime, "to dest", dest)
	}

	if wrote {
		_, err = f.Write([]byte("]"))
		if err != nil {
			os.Remove(dest)
			return newProcessError(fmt.Sprint("writeTimeseriesData: could not write timeseries data file:", dest, "err:", err), true, nil)
		}
	}

	err = f.Close()
	if err != nil {
		os.Remove(dest)
		return newProcessError(fmt.Sprint("writeTimeseriesData: could not close timeseries data file:", dest, "err:", err), true, nil)
	}

	if len(failed) > 0 {
		return newProcessError(fmt.Sprint("writeTimeseriesData: failed to write uuids:", failed), false, failed)
	}
	return nil
}
