package main

import (
	// "fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"
	// "time"
)

const (
	TEST_FILE = "test_file.txt"
)

func testFWStartUp() {
	os.Remove(TEST_FILE)
	testLogStartup()
}

func testFWTeardown() {
	os.Remove(TEST_FILE)
	testLogTeardown()
}

func newTestFileWriter() *FileWriter {
	return newFileWriter(newTestLog())
}

func TestFWWriteUuids(t *testing.T) {
	testFWStartUp()

	fw := newTestFileWriter()
	uuids := []string{"1", "2", "3", "4"}
	fw.writeUuids(TEST_FILE, uuids)

	if !fw.fileExists(TEST_FILE) {
		t.Fatal("file", TEST_FILE, "does not exist")
	}

	body, err := ioutil.ReadFile(TEST_FILE)
	if err != nil {
		t.Fatal("could not read test file")
	}

	stringBody := string(body)
	if stringBody != "[\"1\",\"2\",\"3\",\"4\"]" {
		t.Fatal("file contents does not match expected")
	}

	testFWTeardown()
}

func TestFWWriteMetadata(t *testing.T) {
	testFWStartUp()

	fw := newTestFileWriter()

	dataChan := make(chan *MetadataTuple, 5)

	for i := 0; i < 4; i++ {
		uuid := strconv.Itoa(i)
		tup := &MetadataTuple {
        	uuids: []string{uuid},
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)
	fw.writeMetadata(TEST_FILE, dataChan)

	if !fw.fileExists(TEST_FILE) {
		t.Fatal("file", TEST_FILE, "does not exist")
	}

	body, err := ioutil.ReadFile(TEST_FILE)
	if err != nil {
		t.Fatal("could not read test file")
	}
	stringBody := string(body)
	if stringBody != "[uuid: 0,uuid: 1,uuid: 2,uuid: 3]" {
		t.Fatal("file contents does not match expected")
	} 

	testFWTeardown()
}

func TestFWWriteNoDuplicatesMetadata(t *testing.T) {
	testFWStartUp()

	fw := newTestFileWriter()

	dataChan := make(chan *MetadataTuple, 5)

	for i := 0; i < 4; i++ {
		uuid := strconv.Itoa(i)
		tup := &MetadataTuple {
        	uuids: []string{uuid},
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)
	fw.writeMetadata(TEST_FILE, dataChan)

	dataChan = make(chan *MetadataTuple, 5)

	for i := 0; i < 4; i++ {
		uuid := strconv.Itoa(i)
		tup := &MetadataTuple {
        	uuids: []string{uuid},
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)
	fw.writeMetadata(TEST_FILE, dataChan)

	if !fw.fileExists(TEST_FILE) {
		t.Fatal("file", TEST_FILE, "does not exist")
	}

	body, err := ioutil.ReadFile(TEST_FILE)
	if err != nil {
		t.Fatal("could not read test file")
	}
	stringBody := string(body)
	if stringBody != "[uuid: 0,uuid: 1,uuid: 2,uuid: 3]" {
		t.Fatal("file contents does not match expected")
	} 

	testFWTeardown()
}

func TestFWWriteAndAppendMetadata(t *testing.T) {
	testFWStartUp()

	fw := newTestFileWriter()

	dataChan := make(chan *MetadataTuple, 6)
	var wg sync.WaitGroup
	wg.Add(1)

	var chanWg sync.WaitGroup
	chanWg.Add(1)

	go func() {
		defer wg.Done()
		fw.writeMetadata(TEST_FILE, dataChan)
	}()

	go func() {
		defer chanWg.Done()
		for i := 0; i < 3; i++ {
			uuid := strconv.Itoa(i)
			tup := &MetadataTuple {
	        	uuids: []string{uuid},
	        	data: []byte("uuid: " + uuid),
	    	}
	    	dataChan <- tup
		}
	}()

	chanWg.Wait()

	for i := 3; i < 6; i++ {
		uuid := strconv.Itoa(i)
		tup := &MetadataTuple {
        	uuids: []string{uuid},
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)

	wg.Wait()

	if !fw.fileExists(TEST_FILE) {
		t.Fatal("file", TEST_FILE, "does not exist")
	}

	body, err := ioutil.ReadFile(TEST_FILE)
	if err != nil {
		t.Fatal("could not read test file")
	}
	stringBody := string(body)
	if stringBody != "[uuid: 0,uuid: 1,uuid: 2,uuid: 3,uuid: 4,uuid: 5]" {
		t.Fatal("file contents does not match expected")
	}

	testFWTeardown()
}

func TestFWWriteTimeseriesData(t *testing.T) {
	testFWStartUp()

	fw := newTestFileWriter()

	dataChan := make(chan *TimeseriesTuple, 5)

	for i := 0; i < 4; i++ {
		uuid := strconv.Itoa(i)
		slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(i),
				EndTime: int64(i + 1),
			}

		tup := &TimeseriesTuple {
        	slot: slot,
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)
	fw.writeTimeseriesData(TEST_FILE, dataChan)

	if !fw.fileExists(TEST_FILE) {
		t.Fatal("file", TEST_FILE, "does not exist")
	}

	body, err := ioutil.ReadFile(TEST_FILE)
	if err != nil {
		t.Fatal("could not read test file")
	}
	stringBody := string(body)
	if stringBody != "[uuid: 0,uuid: 1,uuid: 2,uuid: 3]" {
		t.Fatal("file contents does not match expected")
	} 

	testFWTeardown()
}

func TestFWWriteNoDuplicatesTimeseriesData(t *testing.T) {
	testFWStartUp()

	fw := newTestFileWriter()

	dataChan := make(chan *TimeseriesTuple, 5)

	for i := 0; i < 4; i++ {
		uuid := strconv.Itoa(i)
		slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(i),
				EndTime: int64(i + 1),
			}

		tup := &TimeseriesTuple {
        	slot: slot,
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)
	fw.writeTimeseriesData(TEST_FILE, dataChan)

	dataChan = make(chan *TimeseriesTuple, 5)

	for i := 0; i < 4; i++ {
		uuid := strconv.Itoa(i)
		slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(i),
				EndTime: int64(i + 1),
			}

		tup := &TimeseriesTuple {
        	slot: slot,
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)
	fw.writeTimeseriesData(TEST_FILE, dataChan)

	if !fw.fileExists(TEST_FILE) {
		t.Fatal("file", TEST_FILE, "does not exist")
	}

	body, err := ioutil.ReadFile(TEST_FILE)
	if err != nil {
		t.Fatal("could not read test file")
	}
	stringBody := string(body)
	if stringBody != "[uuid: 0,uuid: 1,uuid: 2,uuid: 3]" {
		t.Fatal("file contents does not match expected")
	} 

	testFWTeardown()
}

func TestFWWriteAndAppendTimeseriesData(t *testing.T) {
	testFWStartUp()

	fw := newTestFileWriter()

	dataChan := make(chan *TimeseriesTuple, 6)
	var wg sync.WaitGroup
	wg.Add(1)

	var chanWg sync.WaitGroup
	chanWg.Add(1)

	go func() {
		defer wg.Done()
		fw.writeTimeseriesData(TEST_FILE, dataChan)
	}()

	go func() {
		defer chanWg.Done()
		for i := 0; i < 3; i++ {
			uuid := strconv.Itoa(i)
			slot := &TimeSlot {
					Uuid: uuid,
					StartTime: int64(i),
					EndTime: int64(i + 1),
				}

			tup := &TimeseriesTuple {
	        	slot: slot,
	        	data: []byte("uuid: " + uuid),
	    	}
	    	dataChan <- tup
		}
	}()

	chanWg.Wait()

	for i := 3; i < 6; i++ {
		uuid := strconv.Itoa(i)
		slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(i),
				EndTime: int64(i + 1),
			}

		tup := &TimeseriesTuple {
        	slot: slot,
        	data: []byte("uuid: " + uuid),
    	}
    	dataChan <- tup
	}
	close(dataChan)

	wg.Wait()

	if !fw.fileExists(TEST_FILE) {
		t.Fatal("file", TEST_FILE, "does not exist")
	}

	body, err := ioutil.ReadFile(TEST_FILE)
	if err != nil {
		t.Fatal("could not read test file")
	}
	stringBody := string(body)
	if stringBody != "[uuid: 0,uuid: 1,uuid: 2,uuid: 3,uuid: 4,uuid: 5]" {
		t.Fatal("file contents does not match expected")
	}

	testFWTeardown()
}
