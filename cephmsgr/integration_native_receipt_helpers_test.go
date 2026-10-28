package cephmsgr

import (
	"encoding/json"
	"io"
	"os"
)

// readNativeReceiptFile shares only file IO and bounded JSON decoding. Each
// caller retains its receipt filename, size limit and error, request identity
// checks and native-result interpretation. It sends no fixture request.
func readNativeReceiptFile(path string, limit int64, boundErr error, receipt any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return decodeNativeReceipt(file, limit, boundErr, receipt)
}

func decodeNativeReceipt(reader io.Reader, limit int64, boundErr error, receipt any) error {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(data)) > limit {
		return boundErr
	}
	return json.Unmarshal(data, receipt)
}
