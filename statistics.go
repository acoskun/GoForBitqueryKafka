package main

import (
	"fmt"
	"sync"
	"time"
)

type Statistics struct {
	mu           sync.Mutex
	messageCount int64
	startTime    time.Time
}

func newStatistics() *Statistics {
	return &Statistics{
		startTime: time.Now(),
	}
}

func (stat *Statistics) increment() {
	stat.mu.Lock()
	defer stat.mu.Unlock()
	stat.messageCount++
}

func (stat *Statistics) report() {
	stat.mu.Lock()
	defer stat.mu.Unlock()

	duration := time.Since(stat.startTime)
	rate := float64(stat.messageCount) / duration.Seconds()

	fmt.Printf("Statistics: %d messages processed in %v (%.2f msg/sec)\n",
		stat.messageCount, duration, rate)
}
