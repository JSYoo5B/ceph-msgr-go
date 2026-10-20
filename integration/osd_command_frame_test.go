package integration_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// MaxFrameSize bounds an OSD command's reply. The same stable output
// (`config help`, every option's schema) is requested by clients whose limit
// sits just above and just below its reply frame: the first gets the bytes the
// default client got, the second an unknown outcome caused by the frame limit,
// with no partial output. That client's next command, on a new connection, is
// answered.
func TestCephOSDCommandFrameLimitIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	show := cephmsgr.Command{JSON: []byte(`{"prefix":"config help","format":"json"}`)}
	target := cephmsgr.OSDTarget{ID: base.ID, Address: base.Address}
	reference := mutationConnection(t, c, ctx, base)
	full, err := reference.Command(ctx, show)
	reference.Close()
	if err != nil || full.Code != 0 || len(full.Data) < 256<<10 {
		t.Fatal("reference config help", len(full.Data), err)
	}
	size := uint32(len(full.Data))
	dial := func(limit uint32) *cephmsgr.Client {
		options := integrationOptions(t)
		options.EnableOSD = true
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		options.MaxFrameSize = limit
		client, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal("dial with MaxFrameSize", limit, err)
		}
		t.Cleanup(func() { client.Close() })
		return client
	}

	// Just above: the output plus a small allowance for the reply's front.
	above := dial(size + 1024)
	o, err := above.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	fits, err := o.Command(ctx, show)
	o.Close()
	if err != nil || fits.Code != 0 || !bytes.Equal(fits.Data, full.Data) {
		t.Fatalf("config help under MaxFrameSize %d (output %d bytes): %v", size+1024, size, err)
	}

	// Just below: the reply frame cannot fit.
	below := dial(size - 1024)
	o, err = below.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := o.Command(ctx, show)
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, cephmsgr.ErrLimitExceeded) || refused.Code != 0 || len(refused.Data) != 0 || refused.Submission.Transaction == 0 {
		t.Fatalf("an oversized reply must be an unknown outcome from the frame limit without output: code %d, %d bytes: %v", refused.Code, len(refused.Data), err)
	}
	t.Logf("config help output %d bytes: MaxFrameSize %d answered, %d failed: %v", size, size+1024, size-1024, err)
	if _, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err == nil {
		t.Fatal("the connection that failed on the frame limit silently recovered")
	}
	o.Close()
	o, err = below.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal("reopen after the frame limit", err)
	}
	defer o.Close()
	if small, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil || small.Code != 0 || len(small.Data) == 0 {
		t.Fatal("small command on a new connection", err)
	}
}
