package ttlcache

import (
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestCacheSetGet(t *testing.T) {
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
	cache, err := New[string, string](2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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

func TestNewRejectsInvalidMaxSize(t *testing.T) {
	cache, err := New[string, string](0)
	if err == nil {
		if cache != nil {
			t.Fatal("expected cache to be nil on error")
		}

		t.Fatal("expected error for invalid max size")
	}

	if err != ErrInvalidMaxSize {
		t.Fatalf("expected %v, got %v", ErrInvalidMaxSize, err)
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
			cache, err := New[string, string](1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			tt.setup(cache)

			_, ok := cache.Get(tt.key)
			if ok {
				t.Fatal("expected ok=false")
			}
		})
	}
}

func TestCacheSetReturnsErrCacheFullWhenCacheIsFull(t *testing.T) {
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := cache.Set("user:1", "Maxim", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = cache.Set("user:2", "Ivan", time.Minute)
	if err != ErrCacheFull {
		t.Fatalf("expected %v, got %v", ErrCacheFull, err)
	}

	value, ok := cache.Get("user:1")
	if !ok {
		t.Fatal("expected existing key to remain in cache")
	}

	if value != "Maxim" {
		t.Fatalf("expected %q, got %q", "Maxim", value)
	}
}

func TestCacheSetUpdatesExistingKeyWhenCacheIsFull(t *testing.T) {
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := cache.Set("user:1", "Max", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := cache.Set("user:1", "Ivan", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	value, ok := cache.Get("user:1")
	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "Ivan" {
		t.Fatalf("expected %q, got %q", "Ivan", value)
	}
}

func TestCacheSetUsesFreedSpaceAfterDelete(t *testing.T) {
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := cache.Set("user:1", "Maxim", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cache.Delete("user:1")

	if err := cache.Set("user:2", "Ivan", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	value, ok := cache.Get("user:2")
	if !ok {
		t.Fatal("expected user:2 to exist")
	}

	if value != "Ivan" {
		t.Fatalf("expected %q, got %q", "Ivan", value)
	}
}

func TestCacheSetUsesFreedSpaceAfterCleanup(t *testing.T) {
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := cache.Set("user:1", "Maxim", 10*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	cache.Cleanup()

	if err := cache.Set("user:2", "Ivan", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	value, ok := cache.Get("user:2")
	if !ok {
		t.Fatal("expected user:2 to exist")
	}

	if value != "Ivan" {
		t.Fatalf("expected %q, got %q", "Ivan", value)
	}
}

func TestCacheDelete(t *testing.T) {
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cache.Set("user:1", "Maxim", time.Minute)
	cache.Delete("user:1")

	_, ok := cache.Get("user:1")
	if ok {
		t.Fatal("expected ok=false after delete")
	}
}

func TestCacheDeleteMissingKey(t *testing.T) {
	cache, err := New[string, string](1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cache.Delete("missing")

	_, ok := cache.Get("missing")
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}

func TestCacheCleanupExpiredItems(t *testing.T) {
	cache, err := New[string, string](3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
	const workers = 100

	cache, err := New[int, int](workers)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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

func TestNewWithAutoCleanupRejectsInvalidInterval(t *testing.T) {
	cache, err := NewWithAutoCleanup[string, string](1, 0)
	if errors.Is(err, ErrInvalidMaxSize) {
		t.Fatalf("unexpected error: %v", err)
	}

	if err == nil {
		if cache != nil {
			cache.Close()
		}
		t.Fatal("expected error for invalid cleanup interval")
	}
}

func TestCacheAutoCleanupRemovesExpiredItems(t *testing.T) {
	cache, err := NewWithAutoCleanup[string, string](1, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	cache.Set("user:1", "Maxim", 10*time.Millisecond)

	time.Sleep(40 * time.Millisecond)

	cache.mu.RLock()
	_, ok := cache.items["user:1"]
	cache.mu.RUnlock()

	if ok {
		t.Fatal("expected expired item to be removed by background cleanup")
	}
}

func TestCacheCloseIsIdempotent(t *testing.T) {
	cache, err := NewWithAutoCleanup[string, string](1, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cache.Close()
	cache.Close()
}

func TestCacheExists(t *testing.T) {
	tests := []struct {
		name  string
		setup func(c *Cache[string, string])
		key   string
		want  bool
	}{
		{
			name:  "missing key",
			setup: func(c *Cache[string, string]) {},
			key:   "user:1",
			want:  false,
		},
		{
			name: "existing unexpired key",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", time.Minute)
			},
			key:  "user:1",
			want: true,
		},
		{
			name: "expired key",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", 10*time.Millisecond)
				time.Sleep(20 * time.Millisecond)
			},
			key:  "user:1",
			want: false,
		},
		{
			name: "zero TTL",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", 0)
			},
			key:  "user:1",
			want: false,
		},
		{
			name: "negative TTL",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", -time.Second)
			},
			key:  "user:1",
			want: false,
		},
		{
			name: "after delete",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", 10*time.Millisecond)
				c.Delete("user:1")
			},
			key:  "user:1",
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			cache, err := New[string, string](1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			tt.setup(cache)

			ok := cache.Exists(tt.key)
			if ok != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, ok)
			}
		})
	}
}

func TestNewWithAutoCleanupRejectsInvalidMaxSize(t *testing.T) {
	cache, err := NewWithAutoCleanup[string, string](0, time.Second)
	if err == nil {
		if cache != nil {
			t.Fatal("expected cache to be nil on error")
		}

		t.Fatal("expected error for invalid max size")
	}

	if err != ErrInvalidMaxSize {
		t.Fatalf("expected %v, got %v", ErrInvalidMaxSize, err)
	}
}

func TestNewWithAutoCleanupRejectsInvalidCleanupInterval(t *testing.T) {
	cache, err := NewWithAutoCleanup[string, string](1, 0)
	if err == nil {
		if cache != nil {
			t.Fatal("expected cache to be nil on error")
		}

		t.Fatal("expected error for invalid cleanup interval")
	}

	if err != ErrInvalidInterval {
		t.Fatalf("expected %v, got %v", ErrInvalidInterval, err)
	}
}

func TestCacheKeys(t *testing.T) {
	tests := []struct {
		name  string
		setup func(c *Cache[string, string])
		want  []string
	}{
		{
			name:  "empty slice for empty cache",
			setup: func(c *Cache[string, string]) {},
			want:  []string{},
		},
		{
			name: "returns only active keys",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", time.Minute)
				c.Set("user:2", "Ivan", time.Minute)
			},
			want: []string{"user:1", "user:2"},
		},
		{
			name: "skips expired keys",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", 10*time.Millisecond)
				c.Set("user:2", "Ivan", time.Minute)
				time.Sleep(20 * time.Millisecond)
			},
			want: []string{"user:2"},
		},
		{
			name: "skips zero TTL keys",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", 0)
			},
			want: []string{},
		},
		{
			name: "skips negative TTL keys",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", -time.Second)
			},
			want: []string{},
		},
		{
			name: "skips delete keys",
			setup: func(c *Cache[string, string]) {
				c.Set("user:1", "Maxim", time.Minute)
				c.Set("user:2", "Ivan", time.Minute)
				c.Delete("user:1")
			},
			want: []string{"user:2"},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			cache, err := New[string, string](2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			tt.setup(cache)

			got := cache.Keys()

			sort.Strings(got)
			sort.Strings(tt.want)

			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}
