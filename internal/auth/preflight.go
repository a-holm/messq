// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// dbFileName is the database file the store opens, and -wal/-shm are its SQLite siblings. The
// name is duplicated here rather than imported from internal/store (which auth must not reach),
// and it is a stable contract.
const dbFileName = "messq.db"

// Level is the severity of a [Finding].
type Level uint8

const (
	// LevelWarn is a condition worth flagging but not fatal: the daemon may still start.
	LevelWarn Level = iota
	// LevelFatal is a condition that must refuse startup (exit 78 EX_CONFIG, emitted by the
	// gated serve wiring that owns the exit-code contract).
	LevelFatal
)

// Finding is one result of the preflight audit: what is wrong, and what to type next. It is
// shared verbatim with messq doctor (#30), which renders the same struct the daemon refuses
// on.
type Finding struct {
	Level  Level
	What   string
	Detail string
	Fix    string
}

// Options carries the paths and modes [Preflight] audits. UID is the process euid; it is a
// parameter so tests can inject a foreign owner without root, and so doctor can report for the
// running uid.
type Options struct {
	DataDir     string
	AuthFile    string // "" when no --auth-file is configured
	RequireAuth bool   // a listener requires a bearer token (ClassPublic)
	SocketMode  os.FileMode
	UID         int
}

// Preflight audits the filesystem security posture of issue #16 §7 before listeners open. It
// returns the findings in a fixed order; an empty result means startup may proceed. The fatal
// rows each carry the exact fix command the operator must run.
func Preflight(opts Options) []Finding {
	var out []Finding
	out = append(out, checkDataDir(opts)...)
	out = append(out, checkDBFiles(opts)...)
	out = append(out, checkAuthFile(opts)...)
	out = append(out, checkSocketMode(opts)...)
	out = append(out, checkRoot(opts)...)
	return out
}

func checkDataDir(opts Options) []Finding {
	st, err := os.Stat(opts.DataDir)
	if err != nil {
		return []Finding{{Level: LevelFatal, What: "data dir", Detail: fmt.Sprintf("cannot stat %q: %v", opts.DataDir, err)}}
	}
	if !st.IsDir() {
		return []Finding{{Level: LevelFatal, What: "data dir", Detail: fmt.Sprintf("%q is not a directory", opts.DataDir)}}
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		return []Finding{{
			Level:  LevelFatal,
			What:   "data dir",
			Detail: fmt.Sprintf("%s has mode %04o, want 0700", opts.DataDir, perm),
			Fix:    fmt.Sprintf("chmod 700 %q", opts.DataDir),
		}}
	}
	return nil
}

func checkDBFiles(opts Options) []Finding {
	var out []Finding
	for _, name := range []string{dbFileName, dbFileName + "-wal", dbFileName + "-shm"} {
		path := filepath.Join(opts.DataDir, name)
		st, err := os.Stat(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			continue
		case err != nil:
			out = append(out, Finding{Level: LevelFatal, What: "database file", Detail: fmt.Sprintf("cannot stat %q: %v", path, err)})
			continue
		}
		if perm := st.Mode().Perm(); perm&0o077 != 0 {
			out = append(out, Finding{
				Level:  LevelFatal,
				What:   "database file",
				Detail: fmt.Sprintf("%s has mode %04o, want 0600", path, perm),
				Fix:    fmt.Sprintf("chmod 600 %q", path),
			})
		}
	}
	return out
}

func checkAuthFile(opts Options) []Finding {
	if opts.AuthFile == "" {
		return nil
	}
	var out []Finding

	st, err := os.Stat(opts.AuthFile)
	if err != nil {
		return []Finding{{Level: LevelFatal, What: "auth file", Detail: fmt.Sprintf("cannot stat %q: %v", opts.AuthFile, err)}}
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		out = append(out, Finding{
			Level:  LevelFatal,
			What:   "auth file",
			Detail: fmt.Sprintf("%s has mode %04o, want 0600", opts.AuthFile, perm),
			Fix:    fmt.Sprintf("chmod 600 %q", opts.AuthFile),
		})
	}
	if stat, ok := st.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != opts.UID && int(stat.Uid) != 0 {
		out = append(out, Finding{
			Level:  LevelFatal,
			What:   "auth file",
			Detail: fmt.Sprintf("%s is owned by uid %d, running uid is %d", opts.AuthFile, stat.Uid, opts.UID),
			Fix:    fmt.Sprintf("chown %d %q", opts.UID, opts.AuthFile),
		})
	}

	data, err := os.ReadFile(opts.AuthFile)
	if err != nil {
		return append(out, Finding{Level: LevelFatal, What: "auth file", Detail: fmt.Sprintf("cannot read %q: %v", opts.AuthFile, err)})
	}
	file, err := Parse(opts.AuthFile, bytes.NewReader(data))
	if err != nil {
		// Unparseable is fatal at startup (the reload path keeps the old set instead — #17).
		return append(out, Finding{Level: LevelFatal, What: "auth file", Detail: err.Error()})
	}
	if opts.RequireAuth && len(file.Tokens) == 0 {
		out = append(out, Finding{
			Level:  LevelFatal,
			What:   "auth file",
			Detail: fmt.Sprintf("%s holds no tokens while a listener requires authentication", opts.AuthFile),
			Fix:    fmt.Sprintf("messq auth add <id> --auth-file %q", opts.AuthFile),
		})
	}
	return out
}

func checkSocketMode(opts Options) []Finding {
	if opts.SocketMode == 0 {
		return nil
	}
	if opts.SocketMode.Perm()&0o006 != 0 {
		return []Finding{{
			Level:  LevelFatal,
			What:   "socket mode",
			Detail: fmt.Sprintf("--socket-mode %04o grants other read or write; that is an unauthenticated local path", opts.SocketMode.Perm()),
			Fix:    "--socket-mode must not grant other read or write",
		}}
	}
	return nil
}

func checkRoot(opts Options) []Finding {
	if opts.UID == 0 {
		return []Finding{{
			Level:  LevelWarn,
			What:   "uid",
			Detail: "running as uid 0; containers do it legitimately, but consider User=messq in the systemd unit",
		}}
	}
	return nil
}
