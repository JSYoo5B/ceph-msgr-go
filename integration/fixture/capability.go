package main

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// A fixture capability admits one destructive or test-specific fixture mode.
// The fixture reads the declared capabilities from CEPH_MSGR_TEST_CAPABILITIES
// (comma-separated) and the selected test from CEPH_MSGR_TEST_RUN. It admits a
// mode only when its capability is declared, the selector is exactly one
// anchored test (^TestName$) and the profile meets the capability's
// requirements. It never names the test that owns a mode and does not know
// where the declaration comes from: today a catalog row declares it
// (.github/scenarios/catalog.d), and a declaration beside the test can supply
// the same two values without a fixture change.
//
// Each capability is defined once, beside the mode it admits, by one
// defineCapability call. Those calls are the whole vocabulary. The planner
// keeps no copy of it or of any admission rule: tools/scenario_plan.py
// validate runs TestCatalogRowsAreAdmittedByTheFixture, which passes every
// catalog row through loadProfile as the native workflow forwards it, so a row
// that declares an undefined capability, or a capability on a topology this
// fixture refuses for any reason, fails validation with this fixture's own
// error.
//
// The requirements are space-separated terms over the profile:
//
//	NAME=value  CEPH_MSGR_TEST_NAME, a variable the native workflow forwards,
//	            has this value (an empty value is allowed)
//	+name       capability name is declared and admitted as well
//	!name       capability name is not declared
//
// extra, when not nil, is a further condition on fixture inputs that no row
// parameter carries (for example the local oracle variables).
//
// exclusive, when set, names the fixture with its own default selection whose
// one further exact test this capability admits (defineExclusiveCapability).
type fixtureCapability struct {
	name, summary, requires string
	extra                   func(profile) bool
	exclusive               ownFixture
}

// An ownFixture is a fixture that loadProfile gives its own default selection
// and that runs nothing else unless a capability admits one other exact test
// on it (ownSelection). Its admission asks admitsExclusive, so a new
// capability for one of these fixtures is defined beside its own mode with
// defineExclusiveCapability and edits no shared list of capabilities.
type ownFixture string

const (
	fixtureMDSCommands         ownFixture = "MDS command"
	fixtureMDSCephFS           ownFixture = "MDS CephFS metadata"
	fixtureMDSCephFSCompletion ownFixture = "MDS CephFS completion"
	fixtureMDSCephFSData       ownFixture = "MDS CephFS data"
	fixtureRBD                 ownFixture = "RBD"
	fixtureOSDCachePools       ownFixture = "OSD cache-pool"
	fixtureOSDErasureCapacity  ownFixture = "four-OSD erasure-capacity"
)

// fixtureCapabilities is filled by the defineCapability calls of this package.
var fixtureCapabilities = map[string]*fixtureCapability{}

var capabilityNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// capabilityTermPattern is one requirement term (see fixtureCapability).
var capabilityTermPattern = regexp.MustCompile(`^(?:[A-Z][A-Z0-9_]*=[a-z0-9.-]*|[+!][a-z][a-z0-9]*(-[a-z0-9]+)*)$`)

// exactTestSelector is the only selector form that may carry a capability:
// one anchored top-level test name, which go test matches against that one
// test alone.
var exactTestSelector = regexp.MustCompile(`^\^Test[A-Za-z0-9_]+\$$`)

func defineCapability(name, summary, requires string, extra func(profile) bool) *fixtureCapability {
	if !capabilityNamePattern.MatchString(name) || summary == "" || strings.TrimSpace(requires) == "" {
		panic("invalid fixture capability " + name)
	}
	for _, term := range strings.Fields(requires) {
		if !capabilityTermPattern.MatchString(term) {
			panic("invalid requirement " + term + " of fixture capability " + name)
		}
	}
	if fixtureCapabilities[name] != nil {
		panic("duplicate fixture capability " + name)
	}
	c := &fixtureCapability{name: name, summary: summary, requires: requires, extra: extra}
	fixtureCapabilities[name] = c
	return c
}

// defineExclusiveCapability defines a capability that admits one exact test
// other than the default selection on the fixture f. At most one such
// capability of a fixture may be declared for a run (capabilityTopologyProblem),
// so two of them never need to exclude each other by name.
func defineExclusiveCapability(f ownFixture, name, summary, requires string, extra func(profile) bool) *fixtureCapability {
	if f == "" {
		panic("fixture capability " + name + " names no fixture")
	}
	c := defineCapability(name, summary, requires, extra)
	c.exclusive = f
	return c
}

// admitsExclusive reports whether a declared capability admits one exact test
// other than the default selection on the fixture f.
func (p profile) admitsExclusive(f ownFixture) bool {
	for _, name := range p.declaredCapabilities() {
		if c := fixtureCapabilities[name]; c != nil && c.exclusive == f && p.admits(c) {
			return true
		}
	}
	return false
}

// supported reports whether the profile meets the capability's requirements.
// A declared capability that the profile does not support is refused.
func (c *fixtureCapability) supported(p profile) bool {
	env := p.containerEnvironment()
	for _, term := range strings.Fields(c.requires) {
		switch term[0] {
		case '+':
			if other := fixtureCapabilities[term[1:]]; other == nil || other == c || !p.admits(other) {
				return false
			}
		case '!':
			if slices.Contains(p.declaredCapabilities(), term[1:]) {
				return false
			}
		default:
			name, want, _ := strings.Cut(term, "=")
			if got, ok := env["CEPH_MSGR_TEST_"+name]; !ok || got != want {
				return false
			}
		}
	}
	return c.extra == nil || c.extra(p)
}

// parseCapabilities validates the declared capabilities against the
// vocabulary and the selector and returns them in canonical (sorted) form.
func parseCapabilities(value, selector string) (string, error) {
	if value == "" {
		return "", nil
	}
	names := strings.Split(value, ",")
	for _, name := range names {
		if fixtureCapabilities[name] == nil {
			return "", fmt.Errorf("CEPH_MSGR_TEST_CAPABILITIES names undefined fixture capability %q", name)
		}
	}
	slices.Sort(names)
	if len(slices.Compact(slices.Clone(names))) != len(names) {
		return "", fmt.Errorf("CEPH_MSGR_TEST_CAPABILITIES repeats a capability")
	}
	if !exactTestSelector.MatchString(selector) {
		return "", fmt.Errorf("fixture capabilities %s require CEPH_MSGR_TEST_RUN to select exactly one anchored test (^TestName$), not %q", strings.Join(names, ","), selector)
	}
	return strings.Join(names, ","), nil
}

// capabilityTopologyProblem refuses a declared capability whose requirements
// the loaded profile does not meet.
func capabilityTopologyProblem(p profile) error {
	exclusive := map[ownFixture]string{}
	for _, name := range p.declaredCapabilities() {
		c := fixtureCapabilities[name]
		if c == nil {
			return fmt.Errorf("undefined fixture capability %s", name)
		}
		if c.exclusive != "" {
			if other := exclusive[c.exclusive]; other != "" {
				return fmt.Errorf("fixture capabilities %s and %s each admit the one exclusive test of the %s fixture", other, name, c.exclusive)
			}
			exclusive[c.exclusive] = name
		}
		if !c.supported(p) {
			return fmt.Errorf("fixture capability %s is not supported by this fixture profile (requires %s)", name, c.requires)
		}
	}
	return nil
}

func (p profile) declaredCapabilities() []string {
	if p.capabilities == "" {
		return nil
	}
	return strings.Split(p.capabilities, ",")
}

// declares reports whether the capability is declared for an exact one-test
// selection. It does not check the requirements; admits does.
//
// A fixture sequence (sequence.go) declares it when every member is one
// exact test: each member alone declares it, and the sequence was admitted
// only because every member alone loads the same profile.
func (p profile) declares(c *fixtureCapability) bool {
	if !slices.Contains(p.declaredCapabilities(), c.name) {
		return false
	}
	if p.sequence != "" {
		for _, member := range strings.Fields(p.sequence) {
			if !exactTestSelector.MatchString(member) {
				return false
			}
		}
		return true
	}
	return exactTestSelector.MatchString(p.testRun)
}

// admits is the admission of one capability's mode: declared for exactly one
// anchored test, on a profile that meets its requirements.
func (p profile) admits(c *fixtureCapability) bool {
	return p.declares(c) && c.supported(p)
}

// runsDefault reports whether the profile runs its fixture's own default
// selection, explicitly or by leaving CEPH_MSGR_TEST_RUN empty. The default
// is a property of the profile, not a test admission.
func (p profile) runsDefault() bool {
	return p.defaultRun != "" && p.defaultRun == p.testRun
}

// ownSelection is the exclusivity rule of a fixture with its own default
// selection: the run is that default (or left empty for it), or one exact
// test admitted by a capability that this fixture defines.
func ownSelection(run, selected string, admitted bool) bool {
	return run == "" || run == selected || admitted
}
