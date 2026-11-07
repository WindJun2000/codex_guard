package main

import (
	"sync"
	"time"
)

// affinityBinding is one session → account sticky binding (path A: plugin
// self-maintained affinity, independent of the host's native selector).
type affinityBinding struct {
	AuthID   string
	LastSeen time.Time
}

var (
	affinityMu            sync.Mutex
	affinityBindings      = map[string]affinityBinding{}
	affinitySweepOnce     sync.Once
	affinitySweepStopOnce sync.Once
	affinitySweepStop     chan struct{}
)

func affinityKey(provider, sessionID string) string {
	return provider + "::" + sessionID
}

// affinityLookup returns the bound auth id for a session, honoring the TTL.
func affinityLookup(provider, sessionID string, now time.Time) (string, bool) {
	affinityMu.Lock()
	defer affinityMu.Unlock()
	key := affinityKey(provider, sessionID)
	b, ok := affinityBindings[key]
	if !ok {
		return "", false
	}
	cfg := activeConfig()
	if cfg.SessionAffinityTTLSeconds > 0 && now.Sub(b.LastSeen) > time.Duration(cfg.SessionAffinityTTLSeconds)*time.Second {
		delete(affinityBindings, key)
		return "", false
	}
	return b.AuthID, true
}

func affinityBind(provider, sessionID, authID string, now time.Time) {
	affinityMu.Lock()
	defer affinityMu.Unlock()
	affinityBindings[affinityKey(provider, sessionID)] = affinityBinding{AuthID: authID, LastSeen: now}
}

func affinityForget(provider, sessionID string) {
	affinityMu.Lock()
	defer affinityMu.Unlock()
	delete(affinityBindings, affinityKey(provider, sessionID))
}

// startAffinitySweeper starts the background cleanup loop once. Lookup already
// removes expired bindings lazily; the sweeper bounds the map when sessions
// stop being referenced so it cannot grow without limit.
func startAffinitySweeper() {
	affinitySweepOnce.Do(func() {
		affinitySweepStop = make(chan struct{})
		go affinitySweepLoop()
	})
}

func affinitySweepLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sweepAffinityBindings()
		case <-affinitySweepStop:
			return
		}
	}
}

func sweepAffinityBindings() {
	cfg := activeConfig()
	if cfg.SessionAffinityTTLSeconds <= 0 {
		return
	}
	now := time.Now()
	cutoff := now.Add(-time.Duration(cfg.SessionAffinityTTLSeconds) * time.Second)
	affinityMu.Lock()
	defer affinityMu.Unlock()
	for key, b := range affinityBindings {
		if b.LastSeen.Before(cutoff) {
			delete(affinityBindings, key)
		}
	}
}

func stopAffinitySweeper() {
	affinitySweepStopOnce.Do(func() {
		if affinitySweepStop != nil {
			close(affinitySweepStop)
		}
	})
}
