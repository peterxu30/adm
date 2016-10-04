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

    return &Logger {
        log: db,
    }
}

func (logger *Logger) updateUuidStatus(uuid string, status LogStatus) error {
	buf := convertToByteArray(status)
    return logger.put(UUID_BUCKET, uuid, buf)
}

func (logger *Logger) getUuidStatus(uuid string) string {
    return string(logger.get(UUID_BUCKET, uuid))
}

func (logger *Logger) get(bucket string, key string) []byte {
    var value []byte
    byteBucket := convertToByteArray(bucket)
    keyBucket := convertToByteArray(key)
    logger.log.View(func(tx *bolt.Tx) error {
        b := tx.Bucket(byteBucket)
        value = b.Get(keyBucket)
        return nil
    })
    return value
}

func (logger *Logger) put(bucket string, key string, value []byte) error {
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
