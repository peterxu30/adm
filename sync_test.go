package main

import (
	"sync"
	"testing"
)

func TestSyncInit(t *testing.T) {
	s := newSema(4)
	if s.count() != 0 {
		t.Fatal("sema count should be 0")
	}
}

func TestSyncAcquireSimpleValid(t *testing.T) {
	s := newSema(2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			s.acquire()
		}()
	}
	wg.Wait()
	size := s.count()
	if size != 2 {
		t.Fatal("sema count should be 2 but was", size)
	}
}

func TestSyncAcquireSimpleInvalid(t *testing.T) {

}

func TestSyncAcquireConcurrent(t *testing.T) {

}

func TestSyncReleaseSimpleValid(t *testing.T) {

}

func TestSyncReleaseSimpleEmpty(t *testing.T) {

}

func TestConcurrentOps(t *testing.T) {

}
