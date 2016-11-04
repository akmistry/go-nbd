package client

import (
	"sync"
	"time"
)

type Limiter struct {
	limit uint64
	window time.Duration

	lock sync.Mutex
	bucket uint64
	maxBucket uint64
	lastTime time.Time
}

func NewLimiter(limit uint64, window time.Duration, burstFraction float64) *Limiter {
	maxBucket := uint64(float64(limit) * burstFraction)
	return &Limiter{limit: limit, window: window, bucket: maxBucket, maxBucket: maxBucket,
		lastTime: time.Now()}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (l *Limiter) refill(now time.Time) {
	if l.bucket < l.maxBucket && now.After(l.lastTime) {
		interval := now.Sub(l.lastTime)
		refill := uint64(float64(l.limit) * (float64(interval) / float64(l.window)))
		l.bucket = min(l.bucket + refill, l.maxBucket)
	}
}

func (l *Limiter) Add(val uint64) time.Duration {
	l.lock.Lock()
	defer l.lock.Unlock()

	now := time.Now()

	// Refill bucket
	l.refill(now)

	// If request can be satisfied by the bucket, drain it and return a 0 delay.
	if val <= l.bucket {
		l.bucket -= val
		l.lastTime = now
		return 0
	}

	// Otherwise, take everything from the bucket and return a calculated
	// delay for the residual.
	val -= l.bucket
	l.bucket = 0
	delay := time.Duration((float64(val) / float64(l.limit)) * float64(l.window))
	l.lastTime = now.Add(delay)
	return delay
}

func (l *Limiter) Limit(val uint64) {
	time.Sleep(l.Add(val))
}

func (l *Limiter) Get() uint64 {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.refill(time.Now())
	return l.bucket
}
