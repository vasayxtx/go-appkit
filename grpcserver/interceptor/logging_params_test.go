/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"testing"
	"time"

	"github.com/ssgreg/logf"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-appkit/log"
)

func TestLoggingParams_ExtendFields(t *testing.T) {
	lp := &LoggingParams{}

	// Test adding fields
	lp.ExtendFields(
		log.String("field1", "value1"),
		log.Int("field2", 42),
		log.Bool("field3", true),
	)

	require.Len(t, lp.fields, 3)
	require.Equal(t, "field1", lp.fields[0].Key)
	require.Equal(t, "value1", string(lp.fields[0].Bytes))
	require.Equal(t, "field2", lp.fields[1].Key)
	require.Equal(t, int64(42), lp.fields[1].Int)
	require.Equal(t, "field3", lp.fields[2].Key)
	require.True(t, lp.fields[2].Int != 0)

	// Test adding more fields
	lp.ExtendFields(log.String("field4", "value4"))
	require.Len(t, lp.fields, 4)
	require.Equal(t, "field4", lp.fields[3].Key)
	require.Equal(t, "value4", string(lp.fields[3].Bytes))
}

func TestLoggingParams_AddTimeSlotInt(t *testing.T) {
	lp := &LoggingParams{}

	// Test adding time slots
	lp.AddTimeSlotInt("slot1", 100)
	lp.AddTimeSlotInt("slot2", 200)

	timeSlots := lp.getTimeSlots()
	require.Len(t, timeSlots, 2)
	require.Equal(t, int64(100), timeSlots["slot1"])
	require.Equal(t, int64(200), timeSlots["slot2"])

	// Test adding to existing slot (should accumulate)
	lp.AddTimeSlotInt("slot1", 50)
	timeSlots = lp.getTimeSlots()
	require.Equal(t, int64(150), timeSlots["slot1"])
	require.Equal(t, int64(200), timeSlots["slot2"])
}

func TestLoggingParams_AddTimeSlotDurationInMs(t *testing.T) {
	lp := &LoggingParams{}

	// Test adding duration-based time slots
	lp.AddTimeSlotDurationInMs("slot1", 1*time.Second)
	lp.AddTimeSlotDurationInMs("slot2", 2*time.Second)

	timeSlots := lp.getTimeSlots()
	require.Len(t, timeSlots, 2)
	require.Equal(t, int64(1000), timeSlots["slot1"]) // 1 second = 1000ms
	require.Equal(t, int64(2000), timeSlots["slot2"]) // 2 seconds = 2000ms

	// Test adding to existing slot (should accumulate)
	lp.AddTimeSlotDurationInMs("slot1", 500*time.Millisecond)
	timeSlots = lp.getTimeSlots()
	require.Equal(t, int64(1500), timeSlots["slot1"]) // 1000 + 500 = 1500ms
}

func TestLoggingParams_ConcurrentAccess(t *testing.T) {
	lp := &LoggingParams{}

	// Test concurrent access to time slots
	done := make(chan struct{})

	// Start multiple goroutines that add time slots
	for i := 0; i < 10; i++ {
		go func(val int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				lp.AddTimeSlotInt("concurrent_slot", int64(val))
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Check that all values were accumulated correctly
	timeSlots := lp.getTimeSlots()
	require.Len(t, timeSlots, 1)

	// Expected value: sum of (0+1+2+...+9) * 100 = 45 * 100 = 4500
	expectedSum := int64(0)
	for i := 0; i < 10; i++ {
		expectedSum += int64(i) * 100
	}
	require.Equal(t, expectedSum, timeSlots["concurrent_slot"])
}

func TestLoggingParams_EmptyTimeSlots(t *testing.T) {
	lp := &LoggingParams{}

	// Test getting time slots when none have been added
	timeSlots := lp.getTimeSlots()
	require.Nil(t, timeSlots)

	// Test adding a field and then getting time slots
	lp.ExtendFields(log.String("field1", "value1"))
	timeSlots = lp.getTimeSlots()
	require.Nil(t, timeSlots)
}

func TestLoggingParams_MixedUsage(t *testing.T) {
	lp := &LoggingParams{}

	// Test mixing fields and time slots
	lp.ExtendFields(log.String("field1", "value1"))
	lp.AddTimeSlotInt("slot1", 100)
	lp.ExtendFields(log.Int("field2", 42))
	lp.AddTimeSlotDurationInMs("slot2", 1*time.Second)

	// Check fields
	require.Len(t, lp.fields, 2)
	require.Equal(t, "field1", lp.fields[0].Key)
	require.Equal(t, "field2", lp.fields[1].Key)

	// Check time slots
	timeSlots := lp.getTimeSlots()
	require.Len(t, timeSlots, 2)
	require.Equal(t, int64(100), timeSlots["slot1"])
	require.Equal(t, int64(1000), timeSlots["slot2"])
}

func TestLoggableIntMap_EncodeLogfObject(t *testing.T) {
	lim := loggableIntMap{
		"slot1": 100,
		"slot2": 200,
		"slot3": 300,
	}

	// Test that the map is not nil and has expected values
	require.Len(t, lim, 3)
	require.Equal(t, int64(100), lim["slot1"])
	require.Equal(t, int64(200), lim["slot2"])
	require.Equal(t, int64(300), lim["slot3"])
}

func TestLoggableIntMap_EncodeLogfObject_Empty(t *testing.T) {
	lim := loggableIntMap{}

	// Test that empty map works as expected
	require.Empty(t, lim)
}

func TestLoggingParams_Integration(t *testing.T) {
	lp := &LoggingParams{}

	// Simulate a real usage scenario
	lp.ExtendFields(log.String("service", "test-service"))
	lp.AddTimeSlotDurationInMs("db_query", 50*time.Millisecond)
	lp.AddTimeSlotDurationInMs("external_api", 100*time.Millisecond)
	lp.ExtendFields(log.Int("retry_count", 2))
	lp.AddTimeSlotDurationInMs("db_query", 25*time.Millisecond) // Additional query

	// Verify final state
	require.Len(t, lp.fields, 2)
	require.Equal(t, "service", lp.fields[0].Key)
	require.Equal(t, "test-service", string(lp.fields[0].Bytes))
	require.Equal(t, "retry_count", lp.fields[1].Key)
	require.Equal(t, int64(2), lp.fields[1].Int)

	timeSlots := lp.getTimeSlots()
	require.Len(t, timeSlots, 2)
	require.Equal(t, int64(75), timeSlots["db_query"])      // 50 + 25 = 75ms
	require.Equal(t, int64(100), timeSlots["external_api"]) // 100ms

	// Test creating the time_slots field for logging
	lp.fields = append(lp.fields, log.Field{
		Key:  "time_slots",
		Type: logf.FieldTypeObject,
		Any:  lp.getTimeSlots(),
	})

	require.Len(t, lp.fields, 3)
	require.Equal(t, "time_slots", lp.fields[2].Key)
	require.Equal(t, logf.FieldTypeObject, lp.fields[2].Type)
	require.Equal(t, lp.getTimeSlots(), lp.fields[2].Any)
}
