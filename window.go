package main

import (
	"fmt"
)

type Window struct {
    Uuid string `json:"uuid"`
    Readings [][]float64
}

type TimeSlot struct {
    Uuid string
    StartTime int64
    EndTime int64
    Count int
}

func (window *Window) getTimeSlots() []*TimeSlot {
	fmt.Println(window)
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
            Count: int(reading[1]),
        }
        slots[i] = &slot
    }

    return slots
}