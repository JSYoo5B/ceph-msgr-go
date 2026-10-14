package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	flags := flag.NewFlagSet("encode-osd-mutations", flag.ContinueOnError)
	project := flags.String("project", "../..", "product checkout")
	cache := flags.String("cache", "", "optional persistent native build cache")
	output := flags.String("output", "", "required native proof JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *output == "" {
		return errors.New("encode-osd-mutations requires -output PATH and no positional arguments")
	}
	root, err := filepath.Abs(*project)
	if err != nil {
		return err
	}
	wrapper := filepath.Join(root, "tools", "osd_mutation_native_encoder.cc")
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
	for _, name := range []string{"osd_mutation_native_encoder.py", "osd_mutation_native_encoder.cc"} {
		if err := target.CopyFileToContainer(build, filepath.Join(root, "tools", name), "/tmp/"+name, 0600); err != nil {
			return err
		}
	}
	code, stream, err := target.Exec(build, []string{"python3", "-u", "/tmp/osd_mutation_native_encoder.py", "--build-native", "--work", "/native-cache/work", "--wrapper", "/tmp/osd_mutation_native_encoder.cc"}, containerexec.Multiplexed())
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
	proof, err := readNativeEncoderProof(*output+".log", wrapperHash)
	if err != nil {
		return err
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
	fmt.Println("Native v6 front/data proof:", *output)
	return nil
}

func readNativeEncoderProof(path, wrapperHash string) (map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	var proof map[string]any
	for scanner.Scan() {
		var candidate map[string]any
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		if decoder.Decode(&candidate) == nil && candidate["ceph_commit"] != nil {
			proof = candidate
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if proof["ceph_commit"] != nativeEncoderCommit || proof["image"] != profileDefaultImage || proof["native_payload_encoder_verified"] != true || proof["wrapper_sha256"] != wrapperHash {
		return nil, errors.New("native proof lacks the exact source, image, wrapper or successful encoding receipt")
	}
	fixtures, ok := proof["fixtures"].(map[string]any)
	if !ok || len(fixtures) != 5 {
		return nil, errors.New("native proof lacks five encoder vectors")
	}
	for _, name := range []string{"mutation-native-request-v6-all", "mutation-native-reply-v6-all-success", "mutation-native-reply-v6-exclusive-eexist", "mutation-native-reply-v6-cmpext-mismatch", "mutation-native-reply-v6-truncate"} {
		fixture, ok := fixtures[name].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("native proof lacks %s", name)
		}
		for _, segment := range []string{"front", "data"} {
			h, ok := fixture[segment+"_hex"].(string)
			data, err := hex.DecodeString(h)
			number, numeric := fixture[segment+"_bytes"].(json.Number)
			count, countErr := number.Int64()
			if !ok || err != nil || !numeric || countErr != nil || count != int64(len(data)) || segment == "front" && len(data) == 0 {
				return nil, fmt.Errorf("native proof has invalid %s %s bytes", name, segment)
			}
		}
	}
	return proof, nil
}
