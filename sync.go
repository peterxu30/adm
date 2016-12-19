package main

type Sema struct {
	counter chan int8
}

func newSema(n int) *Sema {
	return &Sema {
		counter: make(chan int8, n),
	}
}

func (s *Sema) acquire() {
	var one int8
	s.counter <- one
}

func (s *Sema) release() {
	if s.isEmpty() {
		return
	}
	<- s.counter
}

func (s *Sema) count() int {
	return len(s.counter)
}

func (s *Sema) isEmpty() bool {
	return s.count() == 0
}

func (s *Sema) close() {
	close(s.counter)
}
