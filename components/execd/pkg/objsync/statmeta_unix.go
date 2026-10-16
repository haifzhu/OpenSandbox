// Copyright 2026 The OpenSandbox Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux || darwin

package objsync

import (
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func fileOwner(fi os.FileInfo) int {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int(st.Uid)
	}
	return 0
}

func fileGroup(fi os.FileInfo) int {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int(st.Gid)
	}
	return 0
}

// writeMetadata applies uid/gid ownership from a manifest entry. Restores run
// as root in the sandbox pod init container; failures to chown on filesystems
// that do not support it are non-fatal (mode is still applied by the caller).
func writeMetadata(path string, uid, gid int) {
	_ = syscall.Chown(path, uid, gid) //nolint:gosec // best-effort ownership restore
}

// lutimes sets the timestamps of a symbolic link itself (without following
// it). macOS/Linux both expose this as UTIME_OMIT-free lutimens; Go's
// syscall.Lutimes covers both.
func lutimes(path string, atime, mtime time.Time) error {
	tv := []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	}
	return unix.Lutimes(path, tv)
}
