// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

//go:build unix

package sqlite

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/go-tram/tram/internal/adapters/buffer"
)

// acquireLock locks the file at path exclusively without waiting. The lock
// belongs to the open file and is released by the kernel when the process
// dies. Within a process, a queue is opened once and shared.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", buffer.ErrInUse, path)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return f, nil
}

func releaseLock(f *os.File) error {
	return errors.Join(syscall.Flock(int(f.Fd()), syscall.LOCK_UN), f.Close())
}
