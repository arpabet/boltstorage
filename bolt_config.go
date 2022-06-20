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

package boltstorage

import (
	"errors"
	"github.com/boltdb/bolt"
	"time"
)

var (

	BucketSeparator = byte(':')

	ErrDatabaseReadOnly = errors.New("readonly")
	ErrInvalidSeek      = errors.New("invalid seek")
	ErrCanceled         = errors.New("operation was canceled")
)


// Option configures bolt using the functional options paradigm
// popularized by Rob Pike and Dave Cheney. If you're unfamiliar with this style,
// see https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html and
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis.
type Option interface {
	apply(*bolt.Options)
}

// OptionFunc implements Option interface.
type optionFunc func(*bolt.Options)

// apply the configuration to the provided config.
func (fn optionFunc) apply(r *bolt.Options) {
	fn(r)
}

// option that do nothing
func WithNope() Option {
	return optionFunc(func(opts *bolt.Options) {
	})
}

// Timeout is the amount of time to wait to obtain a file lock.
// When set to zero it will wait indefinitely. This option is only
// available on Darwin and Linux.
func WithTimeout(value time.Duration) Option {
	return optionFunc(func(opts *bolt.Options) {
		opts.Timeout = value
	})
}
func WithIndefinitelyTimeout(value time.Duration) Option {
	return optionFunc(func(opts *bolt.Options) {
		opts.Timeout = 0
	})
}

// Sets the DB.NoGrowSync flag before memory mapping the file.
func WithNoGrowSync() Option {
	return optionFunc(func(opts *bolt.Options) {
		opts.NoGrowSync = true
	})
}

// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
// grab a shared lock (UNIX).
func WithReadOnly() Option {
	return optionFunc(func(opts *bolt.Options) {
		opts.ReadOnly = true
	})
}

// Sets the DB.MmapFlags flag before memory mapping the file.
func WithMmapFlags(value int) Option {
	return optionFunc(func(opts *bolt.Options) {
		opts.MmapFlags = value
	})
}

// InitialMmapSize is the initial mmap size of the database
// in bytes. Read transactions won't block write transaction
// if the InitialMmapSize is large enough to hold database mmap
// size. (See DB.Begin for more information)
//
// If <=0, the initial map size is 0.
// If initialMmapSize is smaller than the previous database size,
// it takes no effect.
func WithInitialMmapSize(value int) Option {
	return optionFunc(func(opts *bolt.Options) {
		opts.InitialMmapSize = value
	})
}