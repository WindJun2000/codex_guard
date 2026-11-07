package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	quotaPersistMu   sync.Mutex
	quotaPersistOnce sync.Once
	quotaDirty       bool
	quotaLoadedPath  string
	quotaPersistStop chan struct{}
)

// quotaFilePath returns the persistent quota snapshot file. It lives next to
// the plugin settings file so account quota survives container restarts.
func quotaFilePath() string {
	if p := strings.TrimSpace(os.Getenv("CODEX_GUARD_QUOTA_FILE")); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(pluginStatePath()), "codex-guard.quota.json")
}

// markQuotaDirty flags the in-memory quota snapshots as needing a disk write.
func markQuotaDirty() {
	quotaPersistMu.Lock()
	quotaDirty = true
	quotaPersistMu.Unlock()
}

// loadQuotaSnapshots restores persisted quota snapshots into account state. It
// only fills accounts whose in-memory snapshot has no observation yet, so a
// snapshot observed during startup is never clobbered. It reloads when the
// quota file path changes across a reconfigure.
func loadQuotaSnapshots() {
	path := quotaFilePath()
	quotaPersistMu.Lock()
	if quotaLoadedPath == path {
		quotaPersistMu.Unlock()
		return
	}
	quotaLoadedPath = path
	quotaPersistMu.Unlock()

	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var snapshots map[string]QuotaSnapshot
	if err := json.Unmarshal(raw, &snapshots); err != nil {
		logf("warn", "quota snapshot file invalid: %v", err)
		return
	}

	stateMu.Lock()
	defer stateMu.Unlock()
	for id, q := range snapshots {
		if id == "" {
			continue
		}
		st := ensureLocked(id)
		if st.Quota.ObservedAt.IsZero() {
			st.Quota = q
		}
	}
}

// persistQuotaIfDirty writes the current quota snapshots to disk when changed.
func persistQuotaIfDirty() {
	quotaPersistMu.Lock()
	if !quotaDirty {
		quotaPersistMu.Unlock()
		return
	}
	quotaDirty = false
	quotaPersistMu.Unlock()

	snapshots := map[string]QuotaSnapshot{}
	stateMu.Lock()
	for id, st := range globalAccountStates {
		if st.Quota.ObservedAt.IsZero() {
			continue
		}
		cp := st.Quota
		cp.Windows = append([]QuotaWindow(nil), st.Quota.Windows...)
		snapshots[id] = cp
	}
	stateMu.Unlock()

	path := quotaFilePath()
	data, err := json.MarshalIndent(snapshots, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		logf("warn", "persist quota snapshots failed: %v", err)
	}
}

// startQuotaPersister starts the debounced background writer once.
func startQuotaPersister() {
	quotaPersistOnce.Do(func() {
		quotaPersistStop = make(chan struct{})
		go quotaPersistLoop()
	})
}

// stopQuotaPersister halts the background writer (called from shutdown).
func stopQuotaPersister() {
	if quotaPersistStop == nil {
		return
	}
	select {
	case <-quotaPersistStop:
	default:
		close(quotaPersistStop)
	}
}

func quotaPersistLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-quotaPersistStop:
			return
		case <-ticker.C:
			persistQuotaIfDirty()
		}
	}
}
