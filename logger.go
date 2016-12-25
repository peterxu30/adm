package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
)

type LogStatus uint8

/* Enums for possible log statuses */
const (
	NIL LogStatus = iota + 1 //Indicates no entry. No entry in the log should ever have a NIL value.
	NOT_STARTED     
	WRITE_START
	WRITE_COMPLETE
)

const (
	DB_NAME = "adm.db"

	/* Bolt buckets */
	METADATA_BUCKET = "metadata"    //stores state information about the program
	WINDOW_BUCKET = "window_data"
	UUID_METADATA_BUCKET = "uuid_m_status"
	UUID_TIMESERIES_BUCKET = "uuid_t_status"

	/* Metadata bucket keys */
	/* Status of writes to log */
	UUIDS_FETCHED = "uuids_fetched"
	WINDOWS_FETCHED = "windows_fetched"
	/* Status of actual writes */
	UUIDS_WRITTEN = "uuids_written"
	METADATA_WRITTEN = "metadata_written"
	TIMESERIES_WRITTEN = "timeseries_written"
)

type Logger struct {
	log *bolt.DB
}

func newLogger() *Logger {
	return newLoggerWithName(DB_NAME)
}

func newLoggerWithName(name string) *Logger {
	db, err := bolt.Open(name, 0600, nil)
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
	if logger.getLogMetadata(UUIDS_FETCHED) == NIL {
		logger.updateLogMetadata(UUIDS_FETCHED, NOT_STARTED) //to know whether or not to repull uuids
		// fmt.Println("uuids_fetched initialized")
	}

	if logger.getLogMetadata(UUIDS_WRITTEN) == NIL {
		logger.updateLogMetadata(UUIDS_WRITTEN, NOT_STARTED)
	}

	if logger.getLogMetadata(WINDOWS_FETCHED) == NIL {
		logger.updateLogMetadata(WINDOWS_FETCHED, NOT_STARTED)
		// fmt.Println("write_started initialized")
	}

	if logger.getLogMetadata(METADATA_WRITTEN) == NIL {
		logger.updateLogMetadata(METADATA_WRITTEN, NOT_STARTED)
	}

	if logger.getLogMetadata(TIMESERIES_WRITTEN) == NIL {
		logger.updateLogMetadata(TIMESERIES_WRITTEN, NOT_STARTED)
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

func (logger *Logger) getWindowStatus(uuid string) *Window {
	body := logger.get(WINDOW_BUCKET, []byte(uuid))
	return convertFromBinaryToWindow(body)
}

func (logger *Logger) getWindowKeySet() []string {
	byteKeys := logger.keySet(WINDOW_BUCKET)
	keys := make([]string, len(byteKeys))
	for i, byteKey := range byteKeys {
		keys[i] = string(byteKey)
	}
	return keys
}

func (logger *Logger) getWindowEntrySet() []*Window {
	byteEntries := logger.entrySet(WINDOW_BUCKET)
	entries := make([]*Window, len(byteEntries))
	for i, byteEntry := range byteEntries {
		entries[i] = convertFromBinaryToWindow(byteEntry)
	}
	return entries
}

func (logger *Logger) updateWindowStatus(uuid string, window *Window) error {
	var buf []byte
	if window == nil {
		return errors.New("window " + uuid + ": cannot be nil")
	}
	buf = convertToByteArray(*window)
	return logger.put(WINDOW_BUCKET, []byte(uuid), buf)
}

/* Metadata Functions */

func (logger *Logger) getUuidMetadataStatus(uuid string) LogStatus {
	body := logger.get(UUID_METADATA_BUCKET, []byte(uuid))
	return convertFromBinaryToLogStatus(body)
}

func (logger *Logger) getUuidMetadataKeySet() []string {
	byteKeys := logger.keySet(UUID_METADATA_BUCKET)
	keys := make([]string, len(byteKeys))
	for i, byteKey := range byteKeys {
		keys[i] = string(byteKey)
	}
	return keys
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

func (logger *Logger) keySet(bucket string) [][]byte{
	keys := [][]byte{}
	logger.log.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, k)
		}
		return nil
	})
	return keys
}

func (logger *Logger) entrySet(bucket string) [][]byte {
	values := [][]byte{}
	logger.log.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			values = append(values, v)
		}
		return nil
	})
	return values
}

func (logger *Logger) bucketByteSize(bucket string) int64 {
	var size int64
	logger.log.View(func(tx *bolt.Tx) error {
		size = tx.Size()
		return nil
	})
	return size
}

/* General purpose function to convert data of type interface to byte array
 * Used to convert LogStatus to []byte.
 */
func convertToByteArray(data interface{}) []byte {
	if data == nil {
		return nil
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		fmt.Println("encode error:", err)
	}
	return buf.Bytes()
}

/* Converts byte array into WindowData struct */
func convertFromBinaryToWindow(data []byte) *Window {
	if data == nil {
		return nil
	}

	var window Window
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&window)
	if err != nil {
		fmt.Println("convertFromBinaryToWindow err:", err)
	}
	return &window
}

/* Converts byte array into LogStatus */
func convertFromBinaryToLogStatus(data []byte) LogStatus {
	if data == nil {
		return NIL
	}

	var status LogStatus
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&status)
	if err != nil {
		fmt.Println("convertFromBinaryToLogStatus err:", err)
	}
	return status
}
