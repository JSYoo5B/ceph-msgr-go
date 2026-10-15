package testcluster

import (
	"os"
	"path/filepath"
)

// WritePublicEvidence publishes non-secret fixture metadata for the host-side
// diagnostic collector. Container suites may run as root while the collector
// runs as the unprivileged fixture owner. The private fixture directory still
// controls access; do not use this helper for keys or credentials.
//
// Replace the directory entry instead of truncating the old file so a host
// rerun can replace an earlier container-owned receipt. Close before renaming
// for Windows, and set the mode explicitly rather than relying on the umask.
func WritePublicEvidence(path string, data []byte) (err error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".public-evidence-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer func() { _ = os.Remove(temporary) }()
	if err = file.Chmod(0o644); err == nil {
		_, err = file.Write(data)
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(temporary, path)
}
