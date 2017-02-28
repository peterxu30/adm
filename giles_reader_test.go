package main

import (
	"fmt"
	"sync"
	"testing"
)

const (
	TEST_UUID = "174fb37a-5a5f-57a8-bc60-14746cb4656f"
)

func testNRStartup() {
	testFWStartUp()
}

func testNRTeardown() {
	testFWTeardown()
}

func newTestNetworkReader() *NetworkReader {
	return newGilesReader(newTestLog())
}

func TestNRReadUuids(t *testing.T) {
	testNRStartup()

	nr := newTestNetworkReader()
	var ids []string
	ids = nr.readUuids(Url)

	if len(ids) <= 0 {
		t.Fatal("no UUIDs read")
	}

	testNRTeardown()
}

func TestParallelWindowRead(t *testing.T) {
	testNRStartup()

	nr := newTestNetworkReader()
	test_uuids1 := []string{"fea0230b-c64b-53cd-9640-4e5cceebb7a8"}
	test_uuids2 := []string{"fea29569-c3bb-5cc9-9d62-fb7c9d52aa68"}

	var wg sync.WaitGroup
	wg.Add(2)

	windows := make([]*Window, 2)

	go func(windows []*Window) {
		defer wg.Done()
		windows[0] = nr.readWindows(Url, test_uuids1)[0]
	}(windows)

	go func(windows []*Window) {
		defer wg.Done()
		windows[1] = nr.readWindows(Url, test_uuids2)[0]
	}(windows)

	fmt.Println(windows)

	testNRTeardown()
}

func TestTempParallel(t *testing.T) {
	arry := make([]int, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func(arry []int) {
		defer wg.Done()
		arry[0] = 1
	}(arry)

	go func(arry []int) {
		defer wg.Done()
		arry[1] = 2
	}(arry)

	wg.Wait()
	fmt.Println(arry)
	if arry[0] != 1 || arry[1] != 2 {
		t.Fatal("Array contents do not match expected")
	}

	fmt.Println(arry[:1])

}

func TestNRReadMetadata(t *testing.T) {
	testNRStartup()

	nr := newTestNetworkReader()
	fw := newTestFileWriter()
	dataChan := make(chan *MetadataTuple)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		nr.readMetadata(Url, []string{TEST_UUID}, dataChan)
	}()

	go func() {
		defer wg.Done()
		fw.writeMetadata(Url, dataChan)
	}()

	wg.Wait()

	testNRTeardown()
}