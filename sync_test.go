package main

import (
	"runtime"
	"sync"
	"testing"
	"time"
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

func TestSyncAcquireBlocking(t *testing.T) {
	s := newSema(1)
	var wg sync.WaitGroup
	wg.Add(2)
	var time1 time.Time
	var time2 time.Time
	go func() {
		defer wg.Done()
		s.acquire()

		go func() {
			defer wg.Done()
			s.acquire()
			time2 = time.Now()
		}()

		time.Sleep(2 * time.Second)
		time1 = time.Now()
		s.release()
	}()

	wg.Wait()

	if !time1.Before(time2) {
		t.Fatal("time2 should have been after time1")
	}

}

func TestSyncReleaseSimpleValid(t *testing.T) {
	s := newSema(1)
	s.acquire()

	size := s.count()
	if size != 1 {
		t.Fatal("sema count should be 1 but is", size)
	}

	s.release()

	size = s.count()
	if size != 0 {
		t.Fatal("sema count should be 0 but is", size)
	}
}

func TestSyncReleaseSimpleEmpty(t *testing.T) {
	s := newSema(0)
	finished1 := false
	finished2 := false

	go func(finished *bool) {
		s.release()
		*finished = true
	}(&finished1)

	go func(finished *bool) {
		s.release()
		*finished = true
	}(&finished2)

	time.Sleep(4 * time.Second)
	if !(finished1 && finished2) {
		t.Fatal("releasing an empty semaphore timed out", finished1, finished2)
	}
}

func TestSyncBlocking(t *testing.T) {
	s := newSema(5)
	canRelease := false
	baseline := runtime.NumGoroutine() + 1
	numRoutinesMade := 0
	go func(s *Sema, canRelease *bool, numRoutinesMade *int) {
		for i := 0; i < 10; i++ {
			s.acquire()
			go func(s *Sema, canRelease *bool, numRoutinesMade *int) {
				(*numRoutinesMade)++
				for !(*canRelease) {
					time.Sleep(time.Millisecond)
				}
				s.release()
			}(s, canRelease, numRoutinesMade)
		}

	}(s, &canRelease, &numRoutinesMade)

	time.Sleep(3 * time.Second)
	limit := baseline + 5
	total := runtime.NumGoroutine()
	if total > limit && numRoutinesMade != 5 {
		t.Fatal("there should be at most", limit, "go routines but there are", total)
	}
	canRelease = true
}
