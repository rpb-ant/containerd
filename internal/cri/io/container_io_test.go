/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package io

import (
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cioutil "github.com/containerd/containerd/v2/pkg/ioutil"
)

// writerFunc adapts a function to an io.WriteCloser.
type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
func (writerFunc) Close() error                  { return nil }

// WriterGroup holds its lock while it writes, so while the container is busy
// writing output a new attach session has to wait to register its writers.
// Nothing the client sent on stdin may reach the container in the meantime:
// if stdin is already at EOF the session would be ended before it is
// registered, and then never end.
func TestAttachPipesStdinAfterOutputIsRegistered(t *testing.T) {
	var stdinWritten atomic.Bool
	c := &ContainerIO{
		id:          "test",
		stdoutGroup: cioutil.NewWriterGroup(),
		stderrGroup: cioutil.NewWriterGroup(),
		stdioStream: &stdioStream{stdin: writerFunc(func(p []byte) (int, error) {
			stdinWritten.Store(true)
			return len(p), nil
		})},
	}

	// Keep stderr busy: it is registered last, so Attach gets as far as it
	// can without being fully registered.
	writing, release := make(chan struct{}), make(chan struct{})
	c.stderrGroup.Add("log", writerFunc(func(p []byte) (int, error) {
		close(writing)
		<-release
		return len(p), nil
	}))
	go c.stderrGroup.Write([]byte("container output"))
	<-writing

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Attach(t.Context(), AttachOptions{
			Stdin:  strings.NewReader("abcd1234"),
			Stdout: cioutil.NewNopWriteCloser(io.Discard),
			Stderr: cioutil.NewNopWriteCloser(io.Discard),
		})
	}()

	// Give a wrongly ordered copy time to show itself. Attach is parked on
	// the group's lock for the whole wait, so correct code cannot fail here;
	// a very slow machine can only miss a regression.
	time.Sleep(100 * time.Millisecond)
	if stdinWritten.Load() {
		t.Error("stdin reached the container before the session's writers were registered")
	}
	close(release)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Attach did not return after stdin reached EOF")
	}
}
