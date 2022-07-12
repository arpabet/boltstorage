/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
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