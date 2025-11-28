package postgres

import (
	"encoding/binary"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func decodeTuple(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessage, typeMap *pgtype.Map) (map[string]interface{}, error) {
	values := make(map[string]interface{})
	
	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			return nil, fmt.Errorf("tuple column index %d out of range for relation %s", idx, rel.RelationName)
		}
		colDef := rel.Columns[idx]
		colName := colDef.Name

		switch col.DataType {
		case 'n': // Null
			values[colName] = nil
		case 'u': // Unchanged toast
			// We don't have the value. Ideally we should mark it as such.
			// For now, skip or set nil.
			continue
		case 't': // Text formatted
			val, err := decodeText(col.Data, colDef.DataType)
			if err != nil {
				return nil, err
			}
			values[colName] = val
		case 'b': // Binary formatted - pgoutput usually sends text
			// But if we configured it for binary, we'd handle it here.
			// Default is text.
			values[colName] = col.Data
		}
	}
	return values, nil
}

func decodeText(data []byte, oid uint32) (interface{}, error) {
	s := string(data)
	switch oid {
	case 16: // bool
		return s == "t", nil
	case 20, 21, 23: // int2, int4, int8
		// In a real app, use strconv.ParseInt
		return s, nil 
	case 25, 1043: // text, varchar
		return s, nil
	case 1184, 1114: // timestamptz, timestamp
		// Parse postgres timestamp format
		// "2023-01-01 12:00:00.000+00"
		return s, nil
	default:
		return s, nil
	}
}

// Helper to decode uint64 from bytes (if needed)
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
