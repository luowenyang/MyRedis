//go:build darwin

package main

import (
	"errors"
	"log"
	"time"

	"golang.org/x/sys/unix"
)

type FeType int

const (
	AE_READABLE FeType = 1
	AE_WRITABLE FeType = 2
)

type TeType int

const (
	//重复
	AE_NORMAL TeType = 1
	//只执行一次
	AE_ONCE TeType = 2
)

type FileProc func(loop *AeLoop, fd int, extra interface{})
type TimeProc func(loop *AeLoop, id int, extra interface{})
type AeFileEvent struct {
	fd    int
	mask  FeType
	proc  FileProc
	extra interface{}
}
type AeTimeEvent struct {
	id       int
	mask     TeType
	when     int64 //ms
	interval int64 //ms
	proc     TimeProc
	extra    interface{}
	next     *AeTimeEvent
}
type AeLoop struct {
	FileEvents      map[int]*AeFileEvent
	TimeEvents      *AeTimeEvent
	fileEventFd     int
	timeEventNextId int
	stop            bool
}

// todo 这里有问题 源代码是 : var fe2ep [3]uint32 = [3]uint32{0, unix.EPOLLIN, unix.EPOLLOUT}
var fe2ep = [3]int{0, unix.EVFILT_READ, unix.EVFILT_WRITE}

func getFeKey(fd int, mask FeType) int {
	if mask == AE_READABLE {
		return fd
	} else {
		return fd * -1
	}
}

func (loop *AeLoop) getEpollMask(fd int) int {
	var ev int
	if loop.FileEvents[getFeKey(fd, AE_READABLE)] != nil {
		ev |= fe2ep[AE_READABLE]
	}
	if loop.FileEvents[getFeKey(fd, AE_WRITABLE)] != nil {
		ev |= fe2ep[AE_WRITABLE]
	}
	return ev
}

/*
Linux epoll					BSD/macOS kqueue			作用
epoll_create()				kqueue()				创建事件监控实例
epoll_ctl(EPOLL_CTL_ADD)	kevent() + EV_ADD		添加文件描述符到监控列表
epoll_ctl(EPOLL_CTL_MOD)	kevent() + EV_ADD		修改已监控的描述符事件（覆盖原有事件）
epoll_ctl(EPOLL_CTL_DEL)	kevent() + EV_DELETE	移除监控的描述符
epoll_wait()				kevent()				等待事件触发
*/

func (loop *AeLoop) AddFileEvent(fd int, mask FeType, proc FileProc, extra interface{}) {
	//epoll ctl
	// TODO 源代码是 : unix.EPOLL_CTL_ADD
	//ev := loop.getEpollMask(fd)
	// unix.EpollCtl
	//err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	// 注册监听socket事件
	event := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: int16(fe2ep[mask]),
		Flags:  unix.EV_ADD,
	}
	_, err := unix.Kevent(loop.fileEventFd, []unix.Kevent_t{event}, nil, nil)
	if err != nil {
		log.Printf("EpollCtl Error: %v\n", err)
		return
	}
	// ae ctl
	fe := AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}
	loop.FileEvents[getFeKey(fd, mask)] = &fe
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	//epoll ctl
	ev := loop.getEpollMask(fd)
	ev &= ^fe2ep[mask]
	// 注册监听socket事件
	event := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: int16(fe2ep[mask]),
		Flags:  unix.EV_DELETE,
	}
	//err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	_, err := unix.Kevent(loop.fileEventFd, []unix.Kevent_t{event}, nil, nil)
	if err != nil {
		log.Printf("EpollCtl Del Error: %v\n", err)
	}
	//ae ctl
	loop.FileEvents[getFeKey(fd, mask)] = nil
}

func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, proc TimeProc, extra interface{}) int {
	id := loop.timeEventNextId
	loop.timeEventNextId++
	te := AeTimeEvent{
		id:       id,
		mask:     mask,
		interval: interval,
		proc:     proc,
		extra:    extra,
		next:     loop.TimeEvents,
	}
	loop.TimeEvents = &te
	return id
}
func (loop *AeLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEvents
	var prev *AeTimeEvent
	for p != nil {
		if p.id == id {
			if prev == nil {
				loop.TimeEvents = p.next
			} else {
				prev.next = p.next
			}
			p.next = nil
			break
		}
		prev = p
		p = p.next
	}
}

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func AeLoopCreate() (*AeLoop, error) {
	epollFd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	return &AeLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		fileEventFd:     epollFd,
		timeEventNextId: 1,
		stop:            false,
	}, nil
}

func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	for _, te := range tes {
		te.proc(loop, te.id, te.extra)
		if te.mask == AE_ONCE {
			loop.RemoveTimeEvent(te.id)
		} else {
			te.when = GetMsTime() + te.interval
		}
	}
	if len(fes) > 0 {
		log.Println("ae is processing file events")
		for _, fe := range fes {
			fe.proc(loop, fe.fd, fe.extra)
		}
	}
}

func (loop *AeLoop) nearestTime() int64 {
	var nearest int64 = GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	timeout := loop.nearestTime() - GetMsTime()
	var timeoutSpec *unix.Timespec
	if timeout >= 0 {
		timeoutSpec = &unix.Timespec{
			Sec:  0,
			Nsec: timeout * 1e6, // 毫秒转纳秒
		}
	}
	var events [128]unix.Kevent_t
	n, err := unix.Kevent(loop.fileEventFd, nil, events[:], timeoutSpec)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return // 忽略信号中断
		}
		log.Printf("epoll wait Error: %v\n", err)
		return
	}
	// collect file events
	for i := 0; i < n; i++ {
		if events[i].Filter == unix.EVFILT_READ {
			fe := loop.FileEvents[getFeKey(int(events[i].Ident), AE_READABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
		if events[i].Filter == unix.EVFILT_WRITE {
			fe := loop.FileEvents[getFeKey(int(events[i].Ident), AE_WRITABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
	}
	// collect time events
	now := GetMsTime()
	p := loop.TimeEvents
	for p != nil {
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}
	return
}

func (loop *AeLoop) AeMain() {
	for loop.stop != true {
		tes, fes := loop.AeWait()
		loop.AeProcess(tes, fes)
	}
}
