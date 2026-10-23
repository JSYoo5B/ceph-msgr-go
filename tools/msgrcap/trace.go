package main

import "time"

// Client-side correlation trace written by the probe. It records only
// identities, timing and outcomes: no keys, tickets, secrets or payloads.

type traceFile struct {
	Tool        string         `json:"tool"`
	Substitute  string         `json:"capture_substitution"`
	Mode        string         `json:"connection_mode"`
	Entity      string         `json:"entity"`
	Monitor     string         `json:"monitor"`
	Started     time.Time      `json:"started"`
	Finished    time.Time      `json:"finished"`
	Phases      []tracePhase   `json:"phases"`
	Connections []traceConn    `json:"connections"`
	Commands    []traceCommand `json:"commands"`
}

type tracePhase struct {
	Name    string    `json:"name"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
	Result  string    `json:"result"`
}

type traceConn struct {
	Phase     string    `json:"phase"`
	Network   string    `json:"network"`
	Target    string    `json:"target"`
	Local     string    `json:"local,omitempty"`
	Remote    string    `json:"remote,omitempty"`
	DialStart time.Time `json:"dial_started"`
	DialEnd   time.Time `json:"dial_ended"`
	DialError string    `json:"dial_error,omitempty"`
	Closed    time.Time `json:"closed,omitempty"`
}

type traceCommand struct {
	Phase   string    `json:"phase"`
	Prefix  string    `json:"prefix"`
	Attempt int       `json:"attempt"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
	Code    int32     `json:"code"`
	Bytes   int       `json:"result_bytes"`
	Error   string    `json:"error,omitempty"`
}
