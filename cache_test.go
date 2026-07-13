package ttlcache

import (
	"sync"
	"testing"
	"time"
)

func TestCacheSetGet(t *testing.T) {
	cache := New[string, string]()

	cache.Set("user:1", "Maxim", time.Minute)

	value, ok := cache.Get("user:1")
	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "Maxim" {
		t.Fatalf("expected %q, got %q", "Maxim", value)
	}
}

func TestCacheSetOverwriteValue(t *testing.T) {
	cache := New[string, string]()

	cache.Set("user:1", "Max", time.Minute)
	cache.Set("user:1", "Ivan", time.Minute)

	value, ok := cache.Get("user:1")
	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "Ivan" {
		t.Fatalf("expected %q, got %q", "Ivan", value)
	}
}

func TestCacheSetUpdatesTTL(t *testing.T) {
	cache := New[string, string]()

	cache.Set("session:1", "token", 20*time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	cache.Set("session:1", "token", time.Minute)
	time.Sleep(30 * time.Millisecond)

	value, ok := cache.Get("session:1")
	if !ok {
		t.Fatal("expected key to still exist after TTL update")
	}

	if value != "token" {
		t.Fatalf("expected %q, got %q", "token", value)
	}
}

func TestCacheGetReturnsFalseForUnavailableKeys(t *testing.T) {
	tests := []struct {
		name  string
		setup func(cache *Cache[string, string])
		key   string
	}{
		{
			name:  "missing key",
			setup: func(cache *Cache[string, string]) {},
			key:   "user:1",
		},
		{
			name: "expired key",
			setup: func(cache *Cache[string, string]) {
				cache.Set("user:1", "Max", 10*time.Millisecond)
				time.Sleep(30 * time.Millisecond)
			},
			key: "user:1",
		},
		{
			name: "zero TTL",
			setup: func(cache *Cache[string, string]) {
				cache.Set("user:1", "Max", 0)
			},
			key: "user:1",
		},
		{
			name: "negative TTL",
			setup: func(cache *Cache[string, string]) {
				cache.Set("user:1", "Max", -time.Second)
			},
			key: "user:1",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			cache := New[string, string]()

			tt.setup(cache)

			_, ok := cache.Get(tt.key)
			if ok {
				t.Fatal("expected ok=false")
			}
		})
	}
}

func TestCacheDelete(t *testing.T) {
	cache := New[string, string]()

	cache.Set("user:1", "Maxim", time.Minute)
	cache.Delete("user:1")

	_, ok := cache.Get("user:1")
	if ok {
		t.Fatal("expected ok=false after delete")
	}
}

func TestCacheDeleteMissingKey(t *testing.T) {
	cache := New[string, string]()

	cache.Delete("missing")

	_, ok := cache.Get("missing")
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}

func TestCacheCleanupExpiredItems(t *testing.T) {
	cache := New[string, string]()

	cache.Set("user:1", "Maxim", 10*time.Millisecond)
	cache.Set("user:2", "Lisa", 10*time.Millisecond)
	cache.Set("user:3", "Ivan", time.Minute)

	time.Sleep(30 * time.Millisecond)

	cache.Cleanup()

	_, ok := cache.Get("user:1")
	if ok {
		t.Fatal("expected user:1 to be expired")
	}

	_, ok = cache.Get("user:2")
	if ok {
		t.Fatal("expected user:2 to be expired")
	}

	value, ok := cache.Get("user:3")
	if !ok {
		t.Fatal("expected user:3 to exist")
	}

	if value != "Ivan" {
		t.Fatalf("expected %q, got %q", "Ivan", value)
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	cache := New[int, int]()

	const workers = 100

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		i := i

		go func() {
			defer wg.Done()

			cache.Set(i, i*10, time.Minute)

			value, ok := cache.Get(i)
			if !ok {
				t.Errorf("expected key %d to exist", i)
				return
			}

			if value != i*10 {
				t.Errorf("expected %d, got %d", i*10, value)
			}
		}()
	}

	wg.Wait()
}
