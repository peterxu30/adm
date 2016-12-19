package main

import (
	"os"
	"strconv"
	"testing"
)

//testing constants
const (
	TEST_LOG = "test_log.db"
)

func testStartup() {
	os.Remove(TEST_LOG)
}

func testTeardown() {
	os.Remove(TEST_LOG)
}

func newTestLog() *Logger {
	return newLoggerWithName(TEST_LOG)
}

func TestInit(t *testing.T) {
	testStartup()

	log := newTestLog()
	if log == nil {
		t.Fatal("Log was nil")
	}

	uuidStatus := log.getLogMetadata(UUIDS_FETCHED)
	if uuidStatus != NOT_STARTED {
		t.Fatal("UUIDS_FETCHED should have status NOT_STARTED")
	}

	windowStatus := log.getLogMetadata(WINDOWS_FETCHED)
	if windowStatus != NOT_STARTED {
		t.Fatal("WINDOWS_FETCHED should have status NOT_STARTED")
	}

	testTeardown() //better way than calling it at end of every test?
}

func TestUpdateMetadataUuidsFetchedKey(t *testing.T) {
	testStartup()

	log := newTestLog()
	uuidStatus := log.getLogMetadata(UUIDS_FETCHED)
	if uuidStatus != NOT_STARTED {
		t.Fatal("UUIDS_FETCHED should have status NOT_STARTED")
	}

	err := log.updateLogMetadata(UUIDS_FETCHED, WRITE_START)
	if err != nil {
		t.Fatal("updating UUIDS_FETCHED to WRITE_START failed")
	}

	uuidStatus = log.getLogMetadata(UUIDS_FETCHED)
	if uuidStatus != WRITE_START {
		t.Fatal("UUIDS_FETCHED should have status WRITE_START")
	}

	err = log.updateLogMetadata(UUIDS_FETCHED, WRITE_COMPLETE)
	if err != nil {
		t.Fatal("updating UUIDS_FETCHED to WRITE_COMPLETE failed")
	}

	uuidStatus = log.getLogMetadata(UUIDS_FETCHED)
	if uuidStatus != WRITE_COMPLETE {
		t.Fatal("UUIDS_FETCHED should have status WRITE_COMPLETE")
	}

	testTeardown()
}

func TestUpdateMetadataWindowsFetchedKey(t *testing.T) {
	testStartup()

	log := newTestLog()
	windowStatus := log.getLogMetadata(WINDOWS_FETCHED)
	if windowStatus != NOT_STARTED {
		t.Fatal("WINDOWS_FETCHED should have status NOT_STARTED")
	}

	err := log.updateLogMetadata(WINDOWS_FETCHED, WRITE_START)
	if err != nil {
		t.Fatal("updating WINDOWS_FETCHED to WRITE_START failed")
	}

	windowStatus = log.getLogMetadata(WINDOWS_FETCHED)
	if windowStatus != WRITE_START {
		t.Fatal("WINDOWS_FETCHED should have status WRITE_START")
	}

	err = log.updateLogMetadata(WINDOWS_FETCHED, WRITE_COMPLETE)
	if err != nil {
		t.Fatal("updating WINDOWS_FETCHED to WRITE_COMPLETE failed")
	}

	windowStatus = log.getLogMetadata(WINDOWS_FETCHED)
	if windowStatus != WRITE_COMPLETE {
		t.Fatal("WINDOWS_FETCHED should have status WRITE_COMPLETE")
	}

	testTeardown()
}

func TestInsertWindowData(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		reading := make([][]int64, 1)
		reading[0] = []int64{int64(i)}
		window := &WindowData {
			Uuid: uuid,
			Readings: reading,
		}
		err := log.updateWindowStatus(uuid, window)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting window data failed for uuid:", uuid)
		}
	}

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		window := log.getWindowStatus(uuid)
		if window.Uuid != uuid || window.Readings[0][0] != int64(i) {
			t.Fatal("uuid", uuid + ":", "corresponding window do not match")
		}
	}

	testTeardown()
}

func TestInsertSimpleUuidMetadata(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		err := log.updateUuidMetadataStatus(uuid, NOT_STARTED)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if uuidStatus != NOT_STARTED {
			t.Fatal("uuid", uuid + ":", "should have status NOT_STARTED")
		}
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		err := log.updateUuidMetadataStatus(uuid, WRITE_START)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if uuidStatus != WRITE_START {
			t.Fatal("uuid", uuid + ":", "should have status WRITE_START")
		}
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		err := log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if uuidStatus != WRITE_COMPLETE {
			t.Fatal("uuid", uuid + ":", "should have status WRITE_COMPLETE")
		}
	}

	testTeardown()
}

func TestInsertStripedUuidMetadata(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 1000; i ++ {
		uuid := strconv.Itoa(i)
		var err error
		if i % 3 == 0 {
			err = log.updateUuidMetadataStatus(uuid, NOT_STARTED)
		} else if i % 3 == 1 {
			err = log.updateUuidMetadataStatus(uuid, WRITE_START)
		} else {
			err = log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
		}
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	for i := 0; i < 1000; i ++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if (i % 3 == 0) {
			if uuidStatus != NOT_STARTED {
				t.Fatal("uuid", uuid + ":", "should have status NOT_STARTED")
			}
		} else if i % 3 == 1 {
			if uuidStatus != WRITE_START {
				t.Fatal("uuid", uuid + ":", "should have status WRITE_START")
			}
		} else {
			if uuidStatus != WRITE_COMPLETE {
				t.Fatal("uuid", uuid + ":", "should have status WRITE_COMPLETE")
			}
		}
		
	}

	testTeardown()
}

func TestUpdateUuidMetadata(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		err := log.updateUuidMetadataStatus(uuid, NOT_STARTED)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		if log.getUuidMetadataStatus(uuid) != NOT_STARTED {
			t.Fatal("uuid", uuid + ":", "should have status NOT_STARTED")
		}
	}

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		err := log.updateUuidMetadataStatus(uuid, WRITE_START)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		if log.getUuidMetadataStatus(uuid) != WRITE_START {
			t.Fatal("uuid", uuid + ":", "should have status WRITE_START")
		}
	}

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		err := log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		if log.getUuidMetadataStatus(uuid) != WRITE_COMPLETE {
			t.Fatal("uuid", uuid + ":", "should have status WRITE_START")
		}
	}

	testTeardown()
}

func TestInsertSimpleUuidTimeseriesData(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			err := log.updateUuidTimeseriesStatus(slot, NOT_STARTED)
			if err != nil {
				t.Fatal("uuid", uuid + ":", "inserting timeseries data failed")
			}
		}	
	}

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			if log.getUuidTimeseriesStatus(slot) != NOT_STARTED {
				t.Fatal("uuid", uuid + ":", "should have status NOT_STARTED")
			}
		}
	}

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			err := log.updateUuidTimeseriesStatus(slot, WRITE_START)
			if err != nil {
				t.Fatal("uuid", uuid + ":", "inserting timeseries data failed")
			}
		}	
	}

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			if log.getUuidTimeseriesStatus(slot) != WRITE_START {
				t.Fatal("uuid", uuid + ":", "should have status WRITE_START")
			}
		}
	}

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			err := log.updateUuidTimeseriesStatus(slot, WRITE_COMPLETE)
			if err != nil {
				t.Fatal("uuid", uuid + ":", "inserting timeseries data failed")
			}
		}	
	}

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			if log.getUuidTimeseriesStatus(slot) != WRITE_COMPLETE {
				t.Fatal("uuid", uuid + ":", "should have status WRITE_COMPLETE")
			}
		}
	}

	testTeardown()
}

func TestInsertStripedUuidTimeseriesData(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			mod := (i + time) % 3
			var err error
			if mod == 0 {
				err = log.updateUuidTimeseriesStatus(slot, NOT_STARTED)			
			} else if mod == 1 {
				err = log.updateUuidTimeseriesStatus(slot, WRITE_START)
			} else {
				err = log.updateUuidTimeseriesStatus(slot, WRITE_COMPLETE)
			}
			if err != nil {
				t.Fatal("uuid", uuid + ":", "inserting timeseries data failed")
			}
		}
	}

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}

			mod := (i + time) % 3
			if mod == 0 {
				if log.getUuidTimeseriesStatus(slot) != NOT_STARTED {
					t.Fatal("uuid", uuid + ":", "should have status NOT_STARTED")
				}			
			} else if mod == 1 {
				if log.getUuidTimeseriesStatus(slot) != WRITE_START {
					t.Fatal("uuid", uuid + ":", "should have status WRITE_START")
				}
			} else {
				if log.getUuidTimeseriesStatus(slot) != WRITE_COMPLETE {
					t.Fatal("uuid", uuid + ":", "should have status WRITE_COMPLETE")
				}
			}
		}
	}

	testTeardown()
}

func TestRetrieveNonexistentWindowKey(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		reading := make([][]int64, 1)
		reading[0] = []int64{int64(i)}
		window := &WindowData {
			Uuid: uuid,
			Readings: reading,
		}
		err := log.updateWindowStatus(uuid, window)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting window data failed: ")
		}
	}

	uuid := strconv.Itoa(1000)
	if log.getWindowStatus(uuid) != nil {
		t.Fatal("uuid", uuid + ":", "should not exist but it does")
	}

	testTeardown()
}

func TestRetrieveNonexistentUuidMetadataKey(t *testing.T) {
	testStartup()

	log := newTestLog()

	//insert some values
	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		err := log.updateUuidMetadataStatus(uuid, NOT_STARTED)
		if err != nil {
			t.Fatal("uuid", uuid + ":", "inserting metadata failed")
		}
	}

	uuid := strconv.Itoa(100)
	if log.getUuidMetadataStatus(uuid) != NIL {
		t.Fatal("uuid", uuid + ":", "should not exist but it does")	
	}

	testTeardown()
}

func TestRetrieveNonexistentUuidTimeseriesKey(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 30; i++ {
		uuid := strconv.Itoa(i)
		for time := 0; time < 3; time++ {
			slot := &TimeSlot {
				Uuid: uuid,
				StartTime: int64(time),
				EndTime: int64(time + 1),
			}
			err := log.updateUuidTimeseriesStatus(slot, NOT_STARTED)
			if err != nil {
				t.Fatal("uuid", uuid + ":", "inserting timeseries data failed")
			}
		}	
	}

	//bad uuid
	badSlot := &TimeSlot {
				Uuid: strconv.Itoa(30),
				StartTime: int64(1),
				EndTime: int64(2),
	}

	if log.getUuidTimeseriesStatus(badSlot) != NIL {
		t.Fatal("uuid", badSlot.Uuid + ":", "should not exist but it does")
	}

	//bad start time
	badSlot = &TimeSlot {
				Uuid: strconv.Itoa(0),
				StartTime: int64(10),
				EndTime: int64(2),
	}

	if log.getUuidTimeseriesStatus(badSlot) != NIL {
		t.Fatal("uuid", badSlot.Uuid + ":", "should not exist but it does")
	}

	//bad end time
	badSlot = &TimeSlot {
				Uuid: strconv.Itoa(0),
				StartTime: int64(1),
				EndTime: int64(20),
	}

	if log.getUuidTimeseriesStatus(badSlot) != NIL {
		t.Fatal("uuid", badSlot.Uuid + ":", "should not exist but it does")
	}

	testTeardown()
}
