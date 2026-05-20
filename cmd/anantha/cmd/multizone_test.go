package cmd

import (
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	carrier "github.com/anupcshan/anantha/pb"
)

func newTestLoadedValues() *LoadedValues {
	return NewLoadedValues("")
}

func floatConfig(name string, val float32) *carrier.ConfigSetting {
	return &carrier.ConfigSetting{
		Name:       name,
		ConfigType: carrier.ConfigType_CT_FLOAT,
		Value: &carrier.ConfigSetting_FloatValue{
			FloatValue: val,
		},
	}
}

// TestOnChangeRegex_ZoneRT_Matches verifies that the zone discovery regex
// used in HAMQTT.Run() matches exactly the keys for zones 1-8 room temperature.
func TestOnChangeRegex_ZoneRT_Matches(t *testing.T) {
	pattern := regexp.MustCompile("^[1-8]/rt$")

	tests := []struct {
		key     string
		matches bool
	}{
		{"1/rt", true},
		{"2/rt", true},
		{"3/rt", true},
		{"4/rt", true},
		{"5/rt", true},
		{"6/rt", true},
		{"7/rt", true},
		{"8/rt", true},
		{"0/rt", false},   // zone 0 doesn't exist
		{"9/rt", false},   // zone 9 doesn't exist
		{"10/rt", false},  // two-digit zone
		{"system/mode", false},
		{"profile/serial", false},
		{"1/rh", false},   // same zone, different metric
		{"1/htsp", false},
		{"1/clsp", false},
		{"1/currentActivity", false},
		{"sensor/wallControl/rt", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := pattern.MatchString(tt.key)
			if got != tt.matches {
				t.Errorf("MatchString(%q) = %v, want %v", tt.key, got, tt.matches)
			}
		})
	}
}

// TestOnChangeRegex_ZoneRT_FiresCallback verifies that updating a zone's rt
// key triggers the OnChangeRegex callback used for reactive zone discovery.
func TestOnChangeRegex_ZoneRT_FiresCallback(t *testing.T) {
	lv := newTestLoadedValues()
	pattern := regexp.MustCompile("^[1-8]/rt$")

	var mu sync.Mutex
	var discovered []string

	lv.OnChangeRegex(pattern, func(tv TimestampedValue) {
		mu.Lock()
		discovered = append(discovered, tv.value.Name)
		mu.Unlock()
	})

	// Updating zone 3's rt should trigger the callback
	lv.StartLoading("test")
	lv.Update("3/rt", floatConfig("3/rt", 72.5), time.Now(), "test")
	lv.EndLoading("test")

	// Wait briefly for the async goroutine
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(discovered) != 1 || discovered[0] != "3/rt" {
		t.Errorf("discovered = %v, want [3/rt]", discovered)
	}
}

// TestOnChangeRegex_ZoneRT_DoesNotFireForNonZoneKeys verifies that updating
// non-zone keys does not trigger the zone discovery callback.
func TestOnChangeRegex_ZoneRT_DoesNotFireForNonZoneKeys(t *testing.T) {
	lv := newTestLoadedValues()
	pattern := regexp.MustCompile("^[1-8]/rt$")

	var mu sync.Mutex
	var discovered []string

	lv.OnChangeRegex(pattern, func(tv TimestampedValue) {
		mu.Lock()
		discovered = append(discovered, tv.value.Name)
		mu.Unlock()
	})

	lv.StartLoading("test1")
	lv.Update("system/mode", &carrier.ConfigSetting{
		Name:       "system/mode",
		ConfigType: carrier.ConfigType_CT_STRING,
		Value:      &carrier.ConfigSetting_MaybeStrValue{MaybeStrValue: []byte("cool")},
	}, time.Now(), "test1")
	lv.EndLoading("test1")

	lv.StartLoading("test2")
	lv.Update("1/rh", floatConfig("1/rh", 45.0), time.Now(), "test2")
	lv.EndLoading("test2")

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(discovered) != 0 {
		t.Errorf("discovered = %v, want empty", discovered)
	}
}

// TestLoadedValues_MultiZone_IndependentCallbacks verifies that OnChange1
// callbacks for different zones fire independently — the core mechanism
// used by registerCallbacks and MetricsHandler.
func TestLoadedValues_MultiZone_IndependentCallbacks(t *testing.T) {
	lv := newTestLoadedValues()

	var mu sync.Mutex
	updates := map[string]float32{}

	for zone := 1; zone <= maxZones; zone++ {
		zoneStr := fmt.Sprintf("%d", zone)
		lv.OnChange1(fmt.Sprintf("%s/rt", zoneStr), func(tv TimestampedValue) {
			mu.Lock()
			updates[tv.value.Name] = tv.value.GetFloatValue()
			mu.Unlock()
		})
	}

	// Update zones 1 and 4 only
	lv.StartLoading("z1")
	lv.Update("1/rt", floatConfig("1/rt", 70.0), time.Now(), "z1")
	lv.EndLoading("z1")

	lv.StartLoading("z4")
	lv.Update("4/rt", floatConfig("4/rt", 68.5), time.Now(), "z4")
	lv.EndLoading("z4")

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(updates) != 2 {
		t.Errorf("got %d updates, want 2", len(updates))
	}
	if updates["1/rt"] != 70.0 {
		t.Errorf("updates[1/rt] = %v, want 70.0", updates["1/rt"])
	}
	if updates["4/rt"] != 68.5 {
		t.Errorf("updates[4/rt] = %v, want 68.5", updates["4/rt"])
	}
}

// TestTimestampedValue_ToString_ZoneKeys verifies that the zone-specific
// formatting in ToString() works for multi-zone keys (the pattern-matching
// logic that replaced the old exact-match cases for "1/rh", "1/htsp", etc.).
func TestTimestampedValue_ToString_ZoneKeys(t *testing.T) {
	tests := []struct {
		zone string
		key  string
		val  float32
		want string
	}{
		{"1", "1/rt", 72.5, "72.5 F"},
		{"2", "2/rt", 68.0, "68.0 F"},
		{"3", "3/rh", 45.5, "45.5 %"},
		{"4", "4/htsp", 65.0, "65.0 F"},
		{"5", "5/clsp", 78.5, "78.5 F"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			tv := TimestampedValue{
				value:       floatConfig(tt.key, tt.val),
				lastUpdated: time.Now(),
			}
			got := tv.ToString()
			if got != tt.want {
				t.Errorf("ToString() = %q, want %q", got, tt.want)
			}
		})
	}
}
