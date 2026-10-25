package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/testcontainers/testcontainers-go"
	containerexec "github.com/testcontainers/testcontainers-go/exec"
)

const nativeEncoderCommit = "7f793731f1b39eb4f465e960113d2363c311b964"

// The offline encoder is a development oracle. It starts no Ceph daemon and
// installs native build packages only in its owned disposable container.
func nativeEncoderCommand(ctx context.Context, args []string) (result error) {
	return runNativeEncoder(ctx, args, mutationEncoderSpec)
}

func runNativeEncoder(ctx context.Context, args []string, spec nativeEncoderSpec) (result error) {
	flags := flag.NewFlagSet(spec.command, flag.ContinueOnError)
	project := flags.String("project", "../..", "product checkout")
	cache := flags.String("cache", "", "optional persistent native build cache")
	output := flags.String("output", "", "required native proof JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *output == "" {
		return fmt.Errorf("%s requires -output PATH and no positional arguments", spec.command)
	}
	root, err := filepath.Abs(*project)
	if err != nil {
		return err
	}
	wrapper := filepath.Join(root, "tools", spec.wrapper)
	input, err := os.ReadFile(wrapper)
	if err != nil {
		return err
	}
	wrapperHash := fmt.Sprintf("%x", sha256.Sum256(input))
	if *cache == "" {
		*cache, err = os.MkdirTemp("", "ceph-native-encoder-")
		if err != nil {
			return err
		}
		defer func() {
			var cleanup *cleanupFailure
			if !errors.As(result, &cleanup) {
				_ = os.RemoveAll(*cache)
			}
		}()
	} else if err := os.MkdirAll(*cache, 0700); err != nil {
		return err
	}
	*cache, err = filepath.Abs(*cache)
	if err != nil {
		return err
	}
	log, err := os.OpenFile(*output+".log", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	fmt.Println("Native encoder cache:", *cache)
	build, stop := context.WithTimeout(ctx, 28*time.Minute)
	defer stop()
	target, err := testcontainers.GenericContainer(build, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: profileDefaultImage, Entrypoint: []string{"/usr/bin/sleep"}, Cmd: []string{"infinity"},
			Labels: map[string]string{"ceph-msgr-go.native-encoder": "true"},
			Mounts: testcontainers.ContainerMounts{testcontainers.BindMount(*cache, "/native-cache")},
		}, Started: true,
	})
	terminated := false
	terminate := func() error {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if target != nil && !terminated {
			if err := target.Terminate(cleanup); err != nil {
				return &cleanupFailure{cause: err}
			}
			terminated = true
		}
		return nil
	}
	defer func() { result = errors.Join(result, terminate()) }()
	if err != nil {
		return err
	}
	// A shared SDK cache can retain every oracle target. Supply all original
	// wrapper sources during configuration, then build only the selected target.
	files := []string{spec.driver, mutationEncoderSpec.wrapper, xattrEncoderSpec.wrapper, omapEncoderSpec.wrapper, omapWriteEncoderSpec.wrapper, omapCmpEncoderSpec.wrapper, omapRangeEncoderSpec.wrapper, callEncoderSpec.wrapper, allocHintEncoderSpec.wrapper, snapEncoderSpec.wrapper, watchersEncoderSpec.wrapper, mapextEncoderSpec.wrapper, checksumEncoderSpec.wrapper, sparseReadEncoderSpec.wrapper, pgListEncoderSpec.wrapper, watchNotifyEncoderSpec.wrapper, allocHintEncoderSpec.driver}
	files = append(files, backoffEncoderSpec.wrapper, requestV8EncoderSpec.wrapper)
	files = append(files, pgListV8EncoderSpec.wrapper)
	files = append(files, replyV8EncoderSpec.wrapper, mdsMapEncoderSpec.wrapper, mdsSessionEncoderSpec.wrapper, mdsRequestEncoderSpec.wrapper)
	files = append(files, mdsCapsEncoderSpec.wrapper, mdsReconnectEncoderSpec.wrapper)
	files = append(files, truncateEncoderSpec.wrapper)
	if spec.command == writeSameEncoderSpec.command {
		files = append(files, "build_sparse_read_oracle.py", "osd_writesame_oracle.cc", writeSameEncoderSpec.wrapper)
	}
	if spec.command == u64XattrEncoderSpec.command {
		files = append(files, "build_sparse_read_oracle.py", "osd_u64_xattr_oracle.cc", u64XattrEncoderSpec.wrapper)
	}
	if spec.command == sparseReadEncoderSpec.command {
		files = append(files, "build_sparse_read_oracle.py", "osd_sparse_read_oracle.cc")
	}
	for _, name := range files {
		if err := target.CopyFileToContainer(build, filepath.Join(root, "tools", name), "/tmp/"+name, 0600); err != nil {
			return err
		}
	}
	code, stream, err := target.Exec(build, []string{"python3", "-u", "/tmp/" + spec.driver, "--build-native", "--work", "/native-cache/work", "--wrapper", "/tmp/" + spec.wrapper}, containerexec.Multiplexed())
	if stream != nil {
		_, copyErr := io.Copy(log, io.LimitReader(stream, 2<<20))
		err = errors.Join(err, copyErr)
	}
	if err != nil || code != 0 {
		return fmt.Errorf("native encoder exit=%d, build log %s: %w", code, *output+".log", errors.Join(err, errors.New("native encoding failed")))
	}
	if err := log.Close(); err != nil {
		return err
	}
	proof, err := readNativeEncoderProofFor(*output+".log", wrapperHash, spec)
	if err != nil {
		return err
	}
	if spec.mdsMaps || spec.mdsSessions || spec.mdsRequests || spec.mdsCaps || spec.mdsReconnect {
		driver, err := os.ReadFile(filepath.Join(root, "tools", spec.driver))
		if err != nil {
			return err
		}
		if proof["build_driver_sha256"] != fmt.Sprintf("%x", sha256.Sum256(driver)) {
			return errors.New("native MDS proof differs from its compiled driver")
		}
	}
	proof["builder_container_id"] = target.GetContainerID()
	if err := terminate(); err != nil {
		return err
	}
	proof["builder_termination_completed"] = true
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Println("Native front/data/header proof:", *output)
	return nil
}
