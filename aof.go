package main

import (
	"log"
	"os"
)

func stopAppendOnly() {
	flushAppendOnlyFile()

	server.appendfd = nil
	server.appendonly = 0
}

func flushAppendOnlyFile() {

}

func startAppendOnly() int8 {
	server.appendonly = 1
	server.lastfsync = GetMsTime()
	server.appendfd, _ = os.OpenFile(server.appendfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if server.appendfd == nil {
		log.Printf("Used tried to switch on AOF via CONFIG, but I can't open the AOF file: %s\n", server.appendfilename)
		return GODIS_ERR
	}
	defer server.appendfd.Close()
	err := rewriteAppendOnlyFileBackground()
	if err != nil {
		log.Printf("Used tried to switch on AOF via CONFIG, I can't trigger a background AOF rewrite operation. Check the above logs for more info about the error: %v\n", err)
		server.appendonly = 0
		return GODIS_ERR
	}
	return GODIS_OK
}
func rewriteAppendOnlyFileBackground() error {

	return nil
}
