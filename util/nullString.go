package util

import "database/sql"

type NullString struct {
	sql.NullString
}

var (
	NewNullableString  = newNullableString
	GetNullStringValue = getNullStringValue
)

func newNullableString(x string) NullString {
	if x == "" {
		return NullString{}
	}
	return NullString{
		sql.NullString{
			String: x,
			Valid:  true,
		},
	}
}

func getNullStringValue(x NullString) string {
	if x.Valid == false {
		return ""
	}
	return x.String
}
