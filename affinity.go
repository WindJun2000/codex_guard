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
	affinityMu       sync.Mutex
	affinityBindings = map[string]affinityBinding{}
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
