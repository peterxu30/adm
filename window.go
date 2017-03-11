package main

import (
    "time"
)
type Window struct {
    Uuid string `json:"uuid"`
    Readings [][]float64 //API call returns float values. Need to be converted to int64 in TimeSlots.
}

type TimeSlot struct {
    Uuid string
    StartTime int64
    EndTime int64
    Count int64
}

func (window *Window) getTimeSlots() []*TimeSlot {
    var slots = make([]*TimeSlot, len(window.Readings))
    length := len(window.Readings)
    for i := 0; i < length; i++ {
        reading := window.Readings[i]
        startTime := int64(reading[0])
        endTime := int64(-1) //means end time is now
        if i < length - 1 {
            endTime = int64(window.Readings[i + 1][0])
        }

        var slot TimeSlot = TimeSlot {
            Uuid: window.Uuid,
            StartTime: startTime,
            EndTime: endTime,
            Count: int64(reading[1]),
        }
        slots[i] = &slot
    }

    return slots
}

func generateDummyWindows(uuids []string, size int64) (windows []*Window) {
    for _, uuid := range uuids {
        windows = append(windows, generateDummyWindow(uuid, size))
    }
    return
}

func generateDummyWindow(uuid string, size int64) *Window {
    currentTime := time.Now().UnixNano()
    var start int64
    var readings [][]float64
    for start = 0; start < currentTime; start += YEAR_NS {
        readings = append(readings, []float64{float64(start), float64(size), 0, 0})
    } 

    return &Window {
        Uuid: uuid,
        Readings: readings,
    }
}
