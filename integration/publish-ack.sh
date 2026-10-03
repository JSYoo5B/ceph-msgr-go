#!/bin/sh
# Publish a disposable fixture acknowledgement only after creation is complete.
set -eu
test "$#" -eq 1 && test -n "$1" || { echo 'usage: publish-ack.sh ACK_PATH' >&2; exit 2; }
ack_path=$1
case "$ack_path" in /*) ;; *) ack_path=./$ack_path ;; esac
test ! -d "$ack_path" || { echo 'acknowledgement path is a directory' >&2; exit 2; }
ack_tmp=
cleanup() {
    ack_status=$?
    if test -n "$ack_tmp"; then
        rm -f "$ack_tmp" || { test "$ack_status" -ne 0 || ack_status=1; }
    fi
    trap - 0
    exit "$ack_status"
}
trap cleanup 0
trap 'exit 130' INT
trap 'exit 143' TERM
ack_tmp=$(mktemp "${ack_path}.pending.XXXXXX")
# mktemp has created and closed the empty file. After this atomic rename a
# consumer may immediately remove ACK_PATH; the producer never touches it again.
mv -f "$ack_tmp" "$ack_path"
ack_tmp=
