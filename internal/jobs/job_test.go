package jobs

import "testing"

func TestCanTransition(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusQueued}:    true,
		{StatusPending, StatusCancelled}: true,
		{StatusQueued, StatusRunning}:    true,
		{StatusQueued, StatusCancelled}:  true,
		{StatusQueued, StatusQueued}:     true,
		{StatusRunning, StatusSucceeded}: true,
		{StatusRunning, StatusPending}:   true,
		{StatusRunning, StatusFailed}:    true,
		{StatusRunning, StatusCancelled}: true,
		{StatusFailed, StatusPending}:    true,
		{StatusCancelled, StatusPending}: true,
	}
	for _, from := range AllStatuses {
		for _, to := range AllStatuses {
			want := allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestSucceededIsAbsorbing(t *testing.T) {
	for _, to := range AllStatuses {
		if CanTransition(StatusSucceeded, to) {
			t.Fatalf("succeeded must be absorbing, but allows -> %s", to)
		}
	}
}

func TestStatusHelpers(t *testing.T) {
	for _, s := range AllStatuses {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	if Status("bogus").Valid() {
		t.Error("bogus status must be invalid")
	}
	terminal := map[Status]bool{StatusSucceeded: true, StatusFailed: true, StatusCancelled: true}
	for _, s := range AllStatuses {
		if s.Terminal() != terminal[s] {
			t.Errorf("%s.Terminal() = %v", s, s.Terminal())
		}
	}
}

func TestQueueForPriority(t *testing.T) {
	cases := map[int]Queue{1: QueueLow, 3: QueueLow, 4: QueueNormal, 5: QueueNormal, 7: QueueNormal, 8: QueueHigh, 10: QueueHigh}
	for p, want := range cases {
		if got := QueueForPriority(p); got != want {
			t.Errorf("QueueForPriority(%d) = %s, want %s", p, got, want)
		}
	}
}
