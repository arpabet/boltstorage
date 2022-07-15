/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package boltstorage_test

import (
	"github.com/stretchr/testify/require"
	"go.arpabet.com/boltstorage"
	"go.arpabet.com/storage"
	"log"
	"os"
	"testing"
)

func TestPrimitives(t *testing.T) {

	file, err := os.CreateTemp(os.TempDir(), "boltdatabasetest.*.db")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())

	s, err := boltstorage.New("test", file.Name(), os.FileMode(0666))
	require.NoError(t, err)

	defer s.Destroy()

	bucket := "first"

	err = s.Set().ByKey("%s:name", bucket).String("value")
	require.NoError(t, err)

	value, err := s.Get().ByKey("%s:name", bucket).ToString()
	require.NoError(t, err)

	require.Equal(t,"value", value)

	cnt := 0
	err = s.Enumerate().Do(func(entry *storage.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate().ByPrefix("%s:", bucket).Do(func(entry *storage.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate().ByPrefix("%s:n", bucket).Do(func(entry *storage.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate().ByPrefix("%s:name", bucket).Do(func(entry *storage.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate().ByPrefix("%s:n", bucket).Seek("%s:name", bucket).Do(func(entry *storage.RawEntry) bool {
		require.Equal(t, "first:name", string(entry.Key))
		require.Equal(t, "value", string(entry.Value))
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)

	cnt = 0
	err = s.Enumerate().ByPrefix("%s:nothing", bucket).Do(func(entry *storage.RawEntry) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 0, cnt)

}