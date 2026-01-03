package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/birdayz/kstreams/kprocessor"
)

// PunctuationSchedule represents a scheduled punctuator
type PunctuationSchedule struct {
	interval      time.Duration
	punctuateType kprocessor.PunctuationType
	callback      kprocessor.Punctuator
	nextTime      time.Time
	cancelled     bool
	mu            sync.Mutex
}

// Cancel marks this schedule as cancelled
func (s *PunctuationSchedule) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelled = true
}

// IsCancelled returns true if this schedule has been cancelled
func (s *PunctuationSchedule) IsCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelled
}

// punctuationManager manages scheduled punctuators
type PunctuationManager struct {
	schedules     []*PunctuationSchedule
	streamTime    time.Time
	lastWallClock time.Time
	mu            sync.RWMutex
}

// newPunctuationManager creates a new punctuation manager
func NewPunctuationManager() *PunctuationManager {
	return &PunctuationManager{
		schedules:     make([]*PunctuationSchedule, 0),
		streamTime:    time.Time{}, // Zero time initially
		lastWallClock: time.Now(),
	}
}

// schedule adds a new punctuation schedule
func (m *PunctuationManager) schedule(
	interval time.Duration,
	pType kprocessor.PunctuationType,
	callback kprocessor.Punctuator,
) kprocessor.Cancellable {
	m.mu.Lock()
	defer m.mu.Unlock()

	var nextTime time.Time
	if pType == kprocessor.PunctuateByStreamTime {
		nextTime = m.streamTime.Add(interval)
	} else {
		nextTime = time.Now().Add(interval)
	}

	schedule := &PunctuationSchedule{
		interval:      interval,
		punctuateType: pType,
		callback:      callback,
		nextTime:      nextTime,
		cancelled:     false,
	}

	m.schedules = append(m.schedules, schedule)
	return schedule
}

// AdvanceStreamTime updates stream time and triggers stream-time punctuators
func (m *PunctuationManager) AdvanceStreamTime(ctx context.Context, timestamp time.Time) error {
	m.mu.Lock()
	// Update stream time to max timestamp seen
	if timestamp.After(m.streamTime) {
		m.streamTime = timestamp
	}
	currentStreamTime := m.streamTime
	m.mu.Unlock()

	// Check stream time punctuators
	return m.checkPunctuators(ctx, kprocessor.PunctuateByStreamTime, currentStreamTime)
}

// CheckWallClock checks and triggers wall-clock-time punctuators
func (m *PunctuationManager) CheckWallClock(ctx context.Context) error {
	now := time.Now()

	m.mu.Lock()
	m.lastWallClock = now
	m.mu.Unlock()

	return m.checkPunctuators(ctx, kprocessor.PunctuateByWallClockTime, now)
}

// checkPunctuators checks if any punctuators of the given type should fire
func (m *PunctuationManager) checkPunctuators(
	ctx context.Context,
	pType kprocessor.PunctuationType,
	currentTime time.Time,
) error {
	m.mu.RLock()
	schedules := make([]*PunctuationSchedule, len(m.schedules))
	copy(schedules, m.schedules)
	m.mu.RUnlock()

	for _, schedule := range schedules {
		if schedule.IsCancelled() {
			continue
		}

		if schedule.punctuateType != pType {
			continue
		}

		schedule.mu.Lock()
		shouldFire := !currentTime.Before(schedule.nextTime)
		schedule.mu.Unlock()

		if shouldFire {
			// Execute callback
			if err := schedule.callback(ctx, currentTime); err != nil {
				return err
			}

			// Update next fire time
			schedule.mu.Lock()
			schedule.nextTime = currentTime.Add(schedule.interval)
			schedule.mu.Unlock()
		}
	}

	// Remove cancelled schedules
	m.removeCancelled()

	return nil
}

// removeCancelled removes all cancelled schedules
func (m *PunctuationManager) removeCancelled() {
	m.mu.Lock()
	defer m.mu.Unlock()

	filtered := make([]*PunctuationSchedule, 0, len(m.schedules))
	for _, schedule := range m.schedules {
		if !schedule.IsCancelled() {
			filtered = append(filtered, schedule)
		}
	}
	m.schedules = filtered
}

// StreamTime returns the current stream time (max timestamp seen)
func (m *PunctuationManager) StreamTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streamTime
}
