package tenant

import "time"

const (
	// testTimeout is the maximum time to wait for an Eventually assertion.
	testTimeout = 500 * time.Millisecond
	// testPoll is the polling interval used inside Eventually assertions.
	testPoll = 10 * time.Millisecond
)
