package main

import (
	"encoding/json"
	"io/ioutil"
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

func (w *FileWriter) writeUuids(dest string, uuids []string) {
	if w.log.getLogMetadata(UUIDS_WRITTEN) == WRITE_COMPLETE {
		return
	}

	body, err := json.Marshal(uuids)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(dest, body, 0644)
	if err != nil {
		panic(err)
	}
}

func (w *FileWriter) writeMetadata(dest string, dataChan chan *MetadataTuple) {
	if w.log.getLogMetadata(METADATA_WRITTEN) == WRITE_COMPLETE {
		return
	}

	if !w.fileExists(dest) {
		err := ioutil.WriteFile(dest, []byte("["), 0644)
	    if err != nil {
	        panic(err)
	    }
	}

	f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	first := true
	wrote := false
	for tuple := range dataChan {
		if w.log.getUuidMetadataStatus(tuple.uuid) == WRITE_COMPLETE {
			continue
		}
		if !first {
			_, err := f.Write([]byte(","))
			if err != nil {
				panic(err)
			}
		} else {
			first = false
		}

		_, err := f.Write(tuple.data)
		if err != nil {
			panic(err)
		}
		wrote = true
		w.log.updateUuidMetadataStatus(tuple.uuid, WRITE_COMPLETE)
	}

	if wrote {
		_, err = f.Write([]byte("]"))
		if err != nil {
			panic(err)
		}
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}
}

func (w *FileWriter) writeTimeseriesData(dest string, dataChan chan *TimeseriesTuple) {
	if w.log.getLogMetadata(TIMESERIES_WRITTEN) == WRITE_COMPLETE {
		return
	}

	if !w.fileExists(dest) {
		err := ioutil.WriteFile(dest, []byte("["), 0644)
	    if err != nil {
	        panic(err)
	    }
	}

	f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
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
				panic(err)
			}
		} else {
			first = false
		}

		_, err := f.Write(tuple.data)
		if err != nil {
			panic(err)
		}
		wrote = true
		w.log.updateUuidTimeseriesStatus(tuple.slot, WRITE_COMPLETE)
	}

	if wrote {
		_, err = f.Write([]byte("]"))
		if err != nil {
			panic(err)
		}
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}
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
