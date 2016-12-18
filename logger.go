package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
)

type LogStatus uint8

/* Enums for possible log statuses */
const (
	NOT_STARTED     LogStatus = iota + 1
	WRITE_START
	WRITE_COMPLETE
)

const (
	/* Bolt buckets */
	METADATA_BUCKET = "metadata"    //stores state information about the program
	WINDOW_BUCKET = "window_data"
	UUID_METADATA_BUCKET = "uuid_m_status"
	UUID_TIMESERIES_BUCKET = "uuid_t_status"

	/* Metadata bucket keys */
	UUIDS_FETCHED = "uuids_fetched"
	WINDOWS_FETCHED = "windows_fetched"
)

type Logger struct {
	log *bolt.DB
}

func newLogger() *Logger {
	db, err := bolt.Open("adm.db", 0600, nil)
	if err != nil {
		panic(err)
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(UUID_METADATA_BUCKET))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(WINDOW_BUCKET))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(METADATA_BUCKET))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(UUID_TIMESERIES_BUCKET))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	logger := Logger{
		log: db,
	}

	//check if this is first initialization of read_uuids
	if logger.get(METADATA_BUCKET, []byte(UUIDS_FETCHED)) == nil {
		logger.updateLogMetadata(UUIDS_FETCHED, NOT_STARTED) //to know whether or not to repull uuids
		fmt.Println("uuids_fetched initialized")
	}

	if logger.get(METADATA_BUCKET, []byte(WINDOWS_FETCHED)) == nil {
		logger.updateLogMetadata(WINDOWS_FETCHED, NOT_STARTED)
		fmt.Println("write_started initialized")
	}

	return &logger
}

/* Log Metadata Functions */

func (logger *Logger) getLogMetadata(key string) LogStatus {
	body := logger.get(METADATA_BUCKET, []byte(key))
	return convertFromBinaryToLogStatus(body)
}

func (logger *Logger) updateLogMetadata(key string, status LogStatus) error {
	buf := convertToByteArray(status)
	return logger.put(METADATA_BUCKET, []byte(key), buf)
}

/* Window Data Functions */

func (logger *Logger) getWindowStatus(uuid string) *WindowData {
	body := logger.get(WINDOW_BUCKET, []byte(uuid))
	window := convertFromBinaryToWindow(body)
	return &window
}

func (logger *Logger) updateWindowStatus(uuid string, window *WindowData) error {
	buf := convertToByteArray(*window)
	return logger.put(WINDOW_BUCKET, []byte(uuid), buf)
}

/* Metadata Functions */

func (logger *Logger) getUuidMetadataStatus(uuid string) LogStatus {
	body := logger.get(UUID_METADATA_BUCKET, []byte(uuid))
	return convertFromBinaryToLogStatus(body)
}

func (logger *Logger) updateUuidMetadataStatus(uuid string, status LogStatus) error {
	buf := convertToByteArray(status)
	return logger.put(UUID_METADATA_BUCKET, []byte(uuid), buf)
}

/* Timeseries Data Functions */

func (logger *Logger) getUuidTimeseriesStatus(timeSlot *TimeSlot) LogStatus {
	body := logger.get(UUID_TIMESERIES_BUCKET, convertToByteArray(*timeSlot))
	return convertFromBinaryToLogStatus(body)
}

func (logger *Logger) updateUuidTimeseriesStatus(timeSlot *TimeSlot, status LogStatus) error {
	buf := convertToByteArray(status)
	return logger.put(UUID_TIMESERIES_BUCKET, convertToByteArray(*timeSlot), buf)
}

/* Lowest level Logger methods. Should not be called directly. */
func (logger *Logger) get(bucket string, key []byte) []byte {
	var value []byte
	logger.log.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		value = b.Get(key)
		return nil
	})
	return value
}

/* Lowest level Logger methods. Should not be called directly. */
func (logger *Logger) put(bucket string, key []byte, value []byte) error {
	return logger.log.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Put(key, value)
		return err
	})
}

/* Converts byte array into WindowData struct */
func convertFromBinaryToWindow(body []byte) WindowData {
	var window WindowData
	buf := bytes.NewReader(body)
	err := binary.Read(buf, binary.LittleEndian, &window)
	if err != nil {
		fmt.Println("getUuidStatus err: ", err)
	}
	return window
}

/* Converts byte array into LogStatus */
func convertFromBinaryToLogStatus(body []byte) LogStatus {
	var status LogStatus
	buf := bytes.NewReader(body)
	err := binary.Read(buf, binary.LittleEndian, &status)
	if err != nil {
		fmt.Println("getUuidStatus err: ", err)
	}
	return status
}

/* General purpose function to convert data of type interface to byte array
 * Used to convert LogStatus to []byte.
 */
func convertToByteArray(data interface{}) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}
