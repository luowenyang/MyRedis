package main

import (
	"fmt"
	"github.com/jinzhu/copier"
	"log"
	"os"
)

func stopAppendOnly() {
	flushAppendOnlyFile()

	server.appendfd = nil
	server.appendonly = 0
}

const (
	AOF_REWRITE_ITEMS_PER_CMD = 64
)

func flushAppendOnlyFile() {
	if len(server.aofbuf) == 0 {
		return
	}
	if server.appendfd == nil {
		var err error
		server.appendfd, err = os.OpenFile(server.appendfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Error opening AOF file: %v\n", err)
			return
		}
		defer server.appendfd.Close()
		log.Printf("AOF file opened successfully: %s\n", server.appendfilename)
	}
	if _, err := server.appendfd.WriteString(server.aofbuf); err != nil {
		log.Printf("Error writing to AOF file: %v\n", err)
	}
	if err := server.appendfd.Sync(); err != nil {
		log.Printf("Error syncing AOF file: %v\n", err)
	}
	server.appendfd.Close()
	server.appendfd = nil
	server.lastfsync = GetMsTime()
	server.dirty = 0
	log.Printf("AppendOnly file flushed successfully.\n")
	server.aofbuf = ""
	server.appendonly = 0
	server.lastfsync = GetMsTime()
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
	// 模拟fork的COW
	snapshot := MockCOW()
	go func() {
		rewriteAppendOnlyFile(snapshot)
	}()
	return nil
}

func MockCOW() *GodisDB {
	snapshot := GodisDB{}
	err := copier.CopyWithOption(&snapshot, server.db, copier.Option{DeepCopy: true})
	if err != nil {
		return nil
	}
	return &snapshot
}

func rewriteListObject(key, o *Gobj) {
	//count := int64(0)

}

func rewriteSetObject(key, o *Gobj) {
	//count := int64(0)

}

func rewriteAppendOnlyFile(db *GodisDB) int8 {
	tmpfile := fmt.Sprintf("temp-rewriteaof-bg-%d.aof", os.Getpid())
	fd, err := os.OpenFile(tmpfile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Failed open the temp append only file: %v\n", err)
		return GODIS_ERR
	}
	defer fd.Close()
	iter := db.data.NewIterator(true)
	defer iter.Close()
	for key, value, exists := iter.Next(); exists; key, value, exists = iter.Next() {
		expiretime := getExpire(key)
		if value.Type_ == GSTR {
			/* Emit a SET command */
			cmd := "*3\r\n$3\r\nSET\r\n"
			/* Key and value */
			if _, err := fd.WriteString(cmd); err != nil {
				log.Printf("Failed writing to the temporary AOF file: %v\n", err)
			}
			if fwriteBulkObject(fd, key) == GODIS_ERR {
				log.Printf("Failed writing to the temporary AOF file: %v\n", err)
			}
			if fwriteBulkObject(fd, value) == GODIS_ERR {
				log.Printf("Failed writing to the temporary AOF file: %v\n", err)
			}
		} else if value.Type_ == GSET {
			if value.encoding == GODIS_ENCODING_INTSET {
				/* Emit the SADDs needed to rebuild the set */
				// 直接遍历 intset
				// TODO inset
			} else if value.encoding != GODIS_ENCODING_HT {
				// 使用安全迭代器遍历内部 dict
				innerIter := value.Val_.(*Dict).NewIterator(true) // 内层安全迭代器
				items := value.setTypeSize()
				count := int64(0)
				for {
					set_key, _, set_exists := innerIter.Next() // 返回(key, val, exists)
					if !set_exists {
						break
					}
					if count == 0 {
						cmd_items := int(min(items, AOF_REWRITE_ITEMS_PER_CMD))
						if fwriteBulkCount(fd, '$', 2+cmd_items) == GODIS_ERR ||
							fwriteBulkString(fd, "sadd") == GODIS_ERR ||
							fwriteBulkObject(fd, key) == GODIS_ERR {
							log.Printf("Failed writing to the temporary AOF file: %v\n", err)
						}
					}
					if fwriteBulkObject(fd, set_key) == GODIS_ERR {
						log.Printf("Failed writing to the temporary AOF file: %v\n", err)
						return GODIS_ERR
					}
					count++
					items--
					if count == AOF_REWRITE_ITEMS_PER_CMD {
						count = 0
					}
				}
				innerIter.Close() // 确保迭代器关闭
			} else {
				panic("Unknown set encoding")
			}
		} else if value.Type_ == GHASH {
			if value.encoding != GODIS_ENCODING_HT {
				innerIter := value.Val_.(*Dict).NewIterator(true) // 内层安全迭代器
				items := value.setTypeSize()
				count := int64(0)
				for {
					hash_key, hash_val, hash_exists := innerIter.Next() // 返回(key, val, exists)
					if !hash_exists {
						break
					}
					if count == 0 {
						cmd_items := int(min(items, AOF_REWRITE_ITEMS_PER_CMD))
						if fwriteBulkCount(fd, '$', 2+cmd_items) == GODIS_ERR ||
							fwriteBulkString(fd, "hset") == GODIS_ERR {
							log.Printf("Failed writing to the temporary AOF file: %v\n", err)
							return GODIS_ERR
						}
					}
					if fwriteBulkObject(fd, hash_key) == GODIS_ERR || fwriteBulkObject(fd, hash_val) == GODIS_ERR {
						log.Printf("Failed writing to the temporary AOF file: %v\n", err)
						return GODIS_ERR
					}
					count++
					items--
					if count == AOF_REWRITE_ITEMS_PER_CMD {
						count = 0
					}
				}
				innerIter.Close() // 确保迭代器关闭
			} else if value.encoding == GODIS_ENCODING_ZIPMAP {
				panic("Unknown hash encoding")
			} else {
				panic("Unknown hash encoding")
			}
		} else if value.Type_ == GLIST {
			cmd := "*3\r\n$5\r\nRPUSH\r\n"
			if value.encoding == GODIS_ENCODING_LINKEDLIST {
				list := value.Val_.(*List)
				p := list.First()
				for p != nil {
					eleObj := p.Val
					if _, err := fd.WriteString(cmd); err != nil {
						log.Printf("Failed writing to the temporary AOF file: %v\n", err)
					}
					if fwriteBulkObject(fd, key) == GODIS_ERR {
						log.Printf("Failed writing to the temporary AOF file: %v\n", err)
					}
					if fwriteBulkObject(fd, eleObj) == GODIS_ERR {
						log.Printf("Failed writing to the temporary AOF file: %v\n", err)
					}
					// Move to the next node
					p = p.next
				}
			} else {
				panic("Unknown list encoding")
			}
		} else if value.Type_ == GZSET {

		} else {
			panic(fmt.Sprintf("Unknown type %v for key %v", value.Type_, key.StrVal()))
		}
		if expiretime != -1 {
			cmd := "*3\r\n$8\r\nEXPIREAT" + CRLF
			cmd += fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key.StrVal()), key.StrVal(), len(value.StrVal()), value.StrVal())
			if _, err := fd.WriteString(cmd); err != nil {
				log.Printf("Failed writing to the temporary AOF file: %v\n", err)
			}
			if fwriteBulkObject(fd, key) == GODIS_ERR {
				log.Printf("Failed writing to the temporary AOF file: %v\n", err)
			}
			if fwriteBulkLongLong(fd, expiretime, len(key.StrVal())) == GODIS_ERR {
				log.Printf("Failed writing to the temporary AOF file: %v\n", err)
			}

		}
	}
	if os.Rename(tmpfile, server.appendfilename) != nil {
		log.Printf("Failed to rename the temporary AOF file to the final AOF file: %v\n", err)
		return GODIS_ERR
	}
	log.Printf("AppendOnly file rewrite completed successfully.\n")
	return GODIS_OK
}

func catAppendOnlyGenericCommand(buf string, args []*Gobj) string {
	argc := len(args)
	buf = fmt.Sprintf("*%d"+CRLF, argc)
	for i := 0; i < argc; i++ {
		o := getDecodedObject(args[i])
		buf += fmt.Sprintf("$%d"+CRLF, len(o.StrVal()))
		buf += o.StrVal() + CRLF
		//	o.DecrRefCount()
	}
	return buf
}

func FeedAppendOnlyFile(cmd *GodisCommand, args []*Gobj) {
	var buf string
	//tempArgs := make([]*Gobj, 3)
	// 这里不需要select db 因为正常使用的情况下，我们都是使用一个db，所以开发的时候也是就用一个db
	// buf = fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s" + CRLF)
	if cmd.name == "expire" {
		//TODO handle expire command
	} else if cmd.name == "setex" {
		//TODO handle setex command
	} else {
		buf = catAppendOnlyGenericCommand(server.aofbuf, args)
	}
	server.aofbuf = buf
	flushAppendOnlyFile()
}

func loadAppendOnlyFile() {
	if server.appendonly == 0 {
		return
	}
	server.aofbuf = ""
	server.appendfd, _ = os.OpenFile(server.appendfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if server.appendfd == nil {
		log.Printf("Used tried to switch on AOF via CONFIG, but I can't open the AOF file: %s\n", server.appendfilename)
		return
	}

	defer server.appendfd.Close()
}
