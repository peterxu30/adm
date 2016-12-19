package main

import (
	// "fmt"
	"os"
	"strconv"
	"testing"
)

//testing constants
const (
	TEST_LOG = "test_log.db"
	NONSENSE = "nonsenseString"
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
			t.Fatal("inserting window data failed: ", uuid)
		}
	}

	for i := 0; i < 1000; i++ {
		uuid := strconv.Itoa(i)
		window := log.getWindowStatus(uuid)
		if window.Uuid != uuid || window.Readings[0][0] != int64(i) {
			t.Fatal("uuid", uuid, "and corresponding window do not match")
		}
	}

	testTeardown()
}

func TestInsertSimpleUuidMetadata(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		log.updateUuidMetadataStatus(uuid, NOT_STARTED)
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if uuidStatus != NOT_STARTED {
			t.Fatal(uuid, "should have status NOT_STARTED")
		}
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		log.updateUuidMetadataStatus(uuid, WRITE_START)
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if uuidStatus != WRITE_START {
			t.Fatal(uuid, "should have status WRITE_START")
		}
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
	}

	for i := 0; i < 100; i++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if uuidStatus != WRITE_COMPLETE {
			t.Fatal(uuid, "should have status WRITE_COMPLETE")
		}
	}

	testTeardown()
}

func TestInsertStripedUuidMetadata(t *testing.T) {
	testStartup()

	log := newTestLog()

	for i := 0; i < 1000; i ++ {
		uuid := strconv.Itoa(i)
		if i % 3 == 0 {
			log.updateUuidMetadataStatus(uuid, NOT_STARTED)
		} else if i % 3 == 1 {
			log.updateUuidMetadataStatus(uuid, WRITE_START)
		} else {
			log.updateUuidMetadataStatus(uuid, WRITE_COMPLETE)
		}
	}

	for i := 0; i < 1000; i ++ {
		uuid := strconv.Itoa(i)
		uuidStatus := log.getUuidMetadataStatus(uuid)
		if (i % 3 == 0) {
			if uuidStatus != NOT_STARTED {
				t.Fatal(uuid, "should have status NOT_STARTED")
			}
		} else if i % 3 == 1 {
			if uuidStatus != WRITE_START {
				t.Fatal(uuid, "should have status WRITE_START")
			}
		} else {
			if uuidStatus != WRITE_COMPLETE {
				t.Fatal(uuid, "should have status WRITE_COMPLETE")
			}
		}
		
	}

	testTeardown()
}

func TestInsertUuidTimeseriesdata(t *testing.T) {}

func TestUpdateUuidMetadata(t *testing.T) {}

func TestUpdateUuidTimeseriesdata(t *testing.T) {}

func TestRetrieveNonexistentWindowKey(t *testing.T) {}

func TestRetrieveNonexistentUuidMetadataKey(t *testing.T) {}

func TestRetrieveNonexistentUuidTimeseriesKey(t *testing.T) {}

/* Tests to write:
 * 1. Insert/retrieve Window data
 * 2. Insert/retrieve metadata
 * 3. Insert/retrieve timeseries data
 * 4. Update/retrieve metadata
 * 5. Update/retrieve timeseries data
*/
