package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey	[]byte
	txn			*MvccTxn
	iter 		engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		startKey: startKey,
		txn: txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
	scanner.iter.Seek(EncodeKey(startKey, txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.iter != nil {
		scan.iter.Close()
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.iter == nil || !scan.iter.Valid() {
		return nil, nil, nil
	}
	// seek the next key whose ts is less than txn.StartTS
	var key []byte
	var item engine_util.DBItem
	for ;scan.iter.Valid();scan.iter.Next() {
		item = scan.iter.Item()
		key = item.Key()
		ts := decodeTimestamp(key)
		// get result
		if ts <= scan.txn.StartTS {
			break
		}
	}
	
	// get key and value
	key = DecodeUserKey(key)
	// move to next, skip duplicate keys
	for scan.iter.Valid() && bytes.Compare(DecodeUserKey(scan.iter.Item().Key()), key) == 0 {
		scan.iter.Next()
	}
	value,err := item.Value()
	if err != nil {
		return nil, nil, err
	}

	// similar with txn's GetValue
	writeRec, err := ParseWrite(value)
	if err != nil {
		return nil, nil, err
	}
	if writeRec.Kind == WriteKindPut {
		value,err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, writeRec.StartTS))
		if err != nil {
			return nil, nil, err
		}
		return key, value, nil
	} else {
		return key, nil, nil
	}
}
