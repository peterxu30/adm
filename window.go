package main

type Window struct {
    uuid string `json:"uuid"`
    readings [][]int64
}

type TimeSlot struct {
    Uuid string
    StartTime int64
    EndTime int64
    Count int
}

func (window *Window) getTimeSlots() []*TimeSlot {
    var slots = make([]*TimeSlot, len(window.readings))
    length := len(window.readings)
    for i := 0; i < length; i++ {
        reading := window.readings[i]
        startTime := reading[0]
        endTime := int64(-1) //means end time is now
        if i < length - 1 {
            endTime = window.readings[i + 1][0]
        }

        var slot TimeSlot = TimeSlot {
            Uuid: window.uuid,
            StartTime: startTime,
            EndTime: endTime,
            Count: int(reading[1]),
        }
        slots[i] = &slot
    }

    return slots
}