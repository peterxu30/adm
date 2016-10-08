package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
)

type LogStatus uint8

const (
    UNSTARTED = iota + 1
    READ_START
    READ_COMPLETE
    WRITE_START
    WRITE_COMPLETE
)

const (
    UUID_BUCKET = "uuid_status"
    METADATA_BUCKET = "metadata"
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
        _, err := tx.CreateBucketIfNotExists([]byte("uuid_status"))
        if err != nil {
            return fmt.Errorf("create bucket: %s", err)
        }
        return nil
    })

    db.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte("metadata"))
        if err != nil {
            return fmt.Errorf("create bucket: %s", err)
        }
        return nil
    })

    logger := Logger {
    	log: db,
    }

    //check if this is first initialization of read_uuids
	if logger.get(METADATA_BUCKET, "read_uuids") == nil {
    	logger.updateLogMetadata("read_uuids", UNSTARTED) //to know whether or not to repull uuids
    	fmt.Println("read_uuids initialized")
	}

	if logger.get(METADATA_BUCKET, "write_status") == nil {
		logger.updateLogMetadata("write_status", UNSTARTED)
		fmt.Println("write_started initialized")
	}

    return &logger
}

func (logger *Logger) updateLogMetadata(key string, status LogStatus) error {
	buf := convertToByteArray(status)
	return logger.put(METADATA_BUCKET, key, buf)
}

func (logger *Logger) getLogMetadata(key string) LogStatus {
	var status LogStatus
	byteAry := logger.get(METADATA_BUCKET, key)
	// if byteAry == nil {
	// 	fmt.Println("GET LOG METADATA NIL")
	// 	return UNSTARTED
	// }
	buf := bytes.NewReader(byteAry)
	err := binary.Read(buf, binary.LittleEndian, &status)
	if err != nil {
		fmt.Println("getUuidStatus err: ", err)
	}
	return status
}

func (logger *Logger) updateUuidStatus(uuid string, status LogStatus) error {
	buf := convertToByteArray(status)
    return logger.put(UUID_BUCKET, uuid, buf)
}

func (logger *Logger) getUuidStatus(uuid string) LogStatus {
	var status LogStatus
	byteAry := logger.get(UUID_BUCKET, uuid)
	buf := bytes.NewReader(byteAry)
	err := binary.Read(buf, binary.LittleEndian, &status)
	if err != nil {
		fmt.Println("getUuidStatus err: ", err, uuid)
	}
	return status
}

func (logger *Logger) get(bucket string, key string) []byte {
    var value []byte
    // fmt.Println("GET KEY: ", key)
    logger.log.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(bucket))
        value = b.Get([]byte(key))
        return nil
    })
    // fmt.Println("GET: ", value)
    return value
}

func (logger *Logger) put(bucket string, key string, value []byte) error {
	// fmt.Println("LOG PUT", key)
    return logger.log.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(bucket))
        err := b.Put([]byte(key), value)
        return err
    })
}

func convertToByteArray(data interface{}) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}
