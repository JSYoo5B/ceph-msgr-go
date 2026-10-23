module github.com/JSYoo5B/ceph-msgr-go/tools/msgrcap

go 1.24.0

require github.com/JSYoo5B/ceph-msgr-go v0.0.0

require github.com/klauspost/compress v1.18.7 // indirect

// Development-only tool module. It reuses the product packages from this
// checkout and is never imported by the product module.
replace github.com/JSYoo5B/ceph-msgr-go => ../..
