package main

import (
	"regexp"
	"slices"
	"strings"
	"sync"
)

// A fixture input is declared beside the code that reads it:
//
//	var fooInput = defineInput("foo", "0")
//
// declares the row parameter foo, which the native workflow forwards as
// CEPH_MSGR_TEST_FOO (tools/native_env.py), with the fallback "0" when the
// row does not set it. loadProfile reads every declared input into the
// profile, and the code reads its value with p.input(fooInput). A new input
// therefore adds no field to profile, no line to loadSingleProfile and no
// entry to any list: its declaration is its registration.
//
// The inputs that are fields of profile (pinnedInputVariables) are frozen. No
// new input becomes a field, and a declaration of a field's variable is
// refused (TestProfileFieldsArePinned).
type fixtureInput struct {
	name, variable, fallback string
}

var (
	fixtureInputs      = map[string]*fixtureInput{}
	fixtureInputSyntax = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// defineInput declares the fixture input name with its fallback. It panics on
// an invalid name, a second declaration of one name and the variable of a
// pinned profile field.
func defineInput(name, fallback string) *fixtureInput {
	if !fixtureInputSyntax.MatchString(name) {
		panic("invalid fixture input name " + name)
	}
	variable := profileInputPrefix + strings.ToUpper(name)
	if fixtureInputs[variable] != nil {
		panic("duplicate fixture input " + name)
	}
	if slices.Contains(pinnedInputVariables(), variable) {
		panic("fixture input " + name + " is the variable of a profile field, " + variable)
	}
	in := &fixtureInput{name: name, variable: variable, fallback: fallback}
	fixtureInputs[variable] = in
	return in
}

// pinnedInputVariables are the variables loadSingleProfile reads into the
// fields of profile, and the sequence variable loadProfile reads first, as the
// pinned reading itself names them.
var pinnedInputVariables = sync.OnceValue(func() []string {
	read := []string{sequenceEnv}
	loadPinnedFields(func(name string) string {
		read = append(read, name)
		return ""
	})
	slices.Sort(read)
	return slices.Compact(read)
})

// readDeclaredInputs reads every declared input, in variable order, into the
// canonical form profile keeps: "VARIABLE=value" entries separated by NUL,
// so profile stays comparable.
func readDeclaredInputs(getenv func(string) string) string {
	variables := make([]string, 0, len(fixtureInputs))
	for variable := range fixtureInputs {
		variables = append(variables, variable)
	}
	slices.Sort(variables)
	entries := make([]string, 0, len(variables))
	for _, variable := range variables {
		value := getenv(variable)
		if value == "" {
			value = fixtureInputs[variable].fallback
		}
		entries = append(entries, variable+"="+value)
	}
	return strings.Join(entries, "\x00")
}

// declaredInputs is the profile's declared inputs as {variable: value}.
func (p profile) declaredInputs() map[string]string {
	result := map[string]string{}
	if p.inputs == "" {
		return result
	}
	for _, entry := range strings.Split(p.inputs, "\x00") {
		variable, value, _ := strings.Cut(entry, "=")
		result[variable] = value
	}
	return result
}

// input is the value of a declared input in this profile: the row's value,
// or the input's fallback. A profile loaded before the input was declared
// has the fallback as well.
func (p profile) input(in *fixtureInput) string {
	if value, ok := p.declaredInputs()[in.variable]; ok {
		return value
	}
	return in.fallback
}
