package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func fwriteBulkLongLong(fp *os.File, data int64, l int) int8 {
	s := fmt.Sprintf("$%v\r\n%s\r\n", l, strconv.FormatInt(data, 10))
	if _, err := fp.WriteString(s); err != nil {
		log.Printf("Error writing long long to file: %v\n", err)
		return GODIS_ERR
	}
	return GODIS_OK
}

func fwriteBulkString(fp *os.File, s string) int8 {
	s = "$" + strconv.Itoa(len(s)) + CRLF + s + CRLF
	if _, err := fp.WriteString(s); err != nil {
		log.Printf("Error writing bulk string to file: %v\n", err)
		return GODIS_ERR
	}
	return GODIS_OK
}

func fwriteBulkObject(fp *os.File, o *Gobj) int8 {
	if o.encoding == GODIS_ENCODING_INT {
		return fwriteBulkLongLong(fp, o.IntVal(), len(o.StrVal()))
	} else if o.encoding == GODIS_ENCODING_RAW {
		return fwriteBulkString(fp, o.StrVal())
	} else {
		panic("Unknown encoding type")
	}
	return GODIS_ERR
}
