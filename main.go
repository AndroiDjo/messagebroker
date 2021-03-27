package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"runtime"

	_ "net/http/pprof"

	pb "github.com/AndroiDjo/messagebroker/mbproto"
	xxh "github.com/cespare/xxhash"
	"google.golang.org/grpc"
)

type server struct{}

type consub struct {
	matcher   *regexp.Regexp
	consumers []*consumer
}

type consumer struct {
	queue chan *pb.ProduceRequest
	mux   sync.Mutex
}

var directsubsmap map[uint64][]*consumer = make(map[uint64][]*consumer)
var ptrsubsmap map[uint64]*consub = make(map[uint64]*consub)
var onewordkeymap map[uint64][]*consub = make(map[uint64][]*consub)

var msgQueueChan chan *pb.ProduceRequest = make(chan *pb.ProduceRequest, 10000)
var consRegistry []*consumer
var produceCnt int = 0
var consumeCnt int = 0
var gmux sync.Mutex
var ptrmux sync.Mutex
var onewordmux sync.Mutex

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func printStats() {
	for {
		time.Sleep(time.Second * 5)
		fmt.Println(time.Now().UTC())
		PrintMemUsage()
		fmt.Println("gorutines cnt", runtime.NumGoroutine())
		fmt.Println("produceCnt", produceCnt)
		fmt.Println("consumeCnt", consumeCnt)
		fmt.Println("msgQueueChan", len(msgQueueChan))
	}
}

func (s server) Produce(srv pb.MessageBroker_ProduceServer) error {
	//ctx := srv.Context()
	produceCnt++
	for {
		/*select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}*/

		req, err := srv.Recv()
		if err == io.EOF {
			// return will close stream from server side
			log.Println("Produce exit")
			return nil
		}
		if err != nil {
			log.Printf("Produce receive error %v", err)
			return nil
		}

		msgQueueChan <- req
	}
}

func (s server) Consume(srv pb.MessageBroker_ConsumeServer) error {
	//ctx := srv.Context()
	consumeCnt++
	var exitchan chan bool = make(chan bool)
	var closechan chan bool = make(chan bool)
	var msgchan chan *pb.ProduceRequest = make(chan *pb.ProduceRequest, 100)
	var cons consumer = consumer{queue: msgchan}
	consRegistry = append(consRegistry, &cons)

	go func() {
		for {
			m := <-msgchan
			resp := pb.ConsumeResponse{Key: m.Key, Payload: m.Payload}
			srv.Send(&resp)
			/*if err := srv.Send(&resp); err != nil {
				log.Printf("Consume send error %v", err)
			}*/
			select {
			case <-closechan:
				exitchan <- true
				return
			default:
			}
		}
	}()

	go func() {
		for {
			req, err := srv.Recv()
			if err == io.EOF {
				closechan <- true
				return
			}
			if err != nil {
				//log.Printf("Consumer request error %v", err)
				closechan <- true
				return
			}

			switch req.Action {
			case pb.ConsumeRequest_SUBSCRIBE:
				for _, key := range req.Keys {
					keyhash := xxh.Sum64String(key)
					if strings.Contains(key, "#") || strings.Contains(key, "*") {
						//ptrsubsmap
						ptrmux.Lock()
						cs := ptrsubsmap[keyhash]
						ptrmux.Unlock()
						if cs != nil {
							found := false
							for _, con := range cs.consumers {
								if con == &cons {
									found = true
									break
								}
							}
							if !found {
								cs.consumers = append(cs.consumers, &cons)
								// TODO: check to not write back to ptrsubsmap
								ptrmux.Lock()
								ptrsubsmap[keyhash] = cs
								ptrmux.Unlock()
							}
						} else {
							cs = &consub{matcher: makeMatcher(key), consumers: []*consumer{&cons}}
							ptrmux.Lock()
							ptrsubsmap[keyhash] = cs
							ptrmux.Unlock()
							for _, word := range strings.Split(key, ".") {
								if word != "#" && word != "*" {
									wordhash := xxh.Sum64String(word)
									onewordmux.Lock()
									ow := onewordkeymap[wordhash]
									onewordmux.Unlock()
									ow = append(ow, cs)
									onewordmux.Lock()
									onewordkeymap[wordhash] = ow
									onewordmux.Unlock()
								}
							}
						}
					} else {
						found := false
						gmux.Lock()
						consarr := directsubsmap[keyhash]
						gmux.Unlock()
						for _, con := range consarr {
							if con == &cons {
								found = true
								break
							}
						}
						if !found {
							consarr = append(consarr, &cons)
							gmux.Lock()
							directsubsmap[keyhash] = consarr
							gmux.Unlock()
						}
					}
				}
			case pb.ConsumeRequest_UNSUBSCRIBE:
				for _, key := range req.Keys {
					keyhash := xxh.Sum64String(key)
					if strings.Contains(key, "#") || strings.Contains(key, "*") {
						ptrmux.Lock()
						cs := ptrsubsmap[keyhash]
						ptrmux.Unlock()
						if cs != nil {
							/*for i, con := range cs.consumers {
								if con == &cons {
									clen := len(cs.consumers)
									cs.consumers[i] = cs.consumers[clen-1]
									cs.consumers = cs.consumers[:clen-1]
									ptrmux.Lock()
									ptrsubsmap[key] = cs
									ptrmux.Unlock()
									break
								}
							}*/
							for _, word := range strings.Split(key, ".") {
								if word != "#" && word != "*" {
									wordhash := xxh.Sum64String(word)
									onewordmux.Lock()
									ow := onewordkeymap[wordhash]
									onewordmux.Unlock()
									for i, con := range ow {
										if con == cs {
											clen := len(ow)
											ow[i] = ow[clen-1]
											ow = ow[:clen-1]
											onewordmux.Lock()
											onewordkeymap[wordhash] = ow
											onewordmux.Unlock()
											break
										}
									}
								}
							}
							cs = nil
							ptrmux.Lock()
							ptrsubsmap[keyhash] = nil
							ptrmux.Unlock()
						}
					} else {
						found := false
						gmux.Lock()
						consarr := directsubsmap[keyhash]
						gmux.Unlock()
						for i, con := range consarr {
							if con == &cons {
								conlen := len(consarr)
								consarr[i] = consarr[conlen-1]
								consarr = consarr[:conlen-1]
								found = true
								break
							}
						}
						if found {
							gmux.Lock()
							directsubsmap[keyhash] = consarr
							gmux.Unlock()
						}
					}
				}
			}
		}
	}()

	select {
	/*case <-ctx.Done():
	return ctx.Err()*/
	case <-exitchan:
		return nil
	}
}

func processMsgQueue() {
	for {
		req := <-msgQueueChan
		if req != nil {
			keyhash := xxh.Sum64String(req.Key)
			gmux.Lock()
			consarr, ok := directsubsmap[keyhash]
			gmux.Unlock()
			if ok {
				for _, cons := range consarr {
					cons.queue <- req
				}
			}

			constosend := make([]*consumer, 1)
			for _, word := range strings.Split(req.Key, ".") {
				wordhash := xxh.Sum64String(word)
				onewordmux.Lock()
				ow := onewordkeymap[wordhash]
				onewordmux.Unlock()
				for _, con := range ow {
					if con != nil {
						if con.matcher.MatchString(req.Key) {
							for _, c := range con.consumers {
								found := false
								for _, cts := range constosend {
									if cts == c {
										found = true
										break
									}
								}

								if !found {
									constosend = append(constosend, c)
								}
							}
						}
					}
				}
			}
			for _, cons := range constosend {
				if cons != nil {
					cons.queue <- req
				}
			}
		}
	}
}

/*
for _, sub := range cons.subtcell {
					if sub != nil {
						match := false
						if sub.matcher != nil {
							if sub.matcher.MatchString(req.Key) {
								match = true
							}
						} else if sub.key == req.Key {
							match = true
						}
						if match {
							cons.queue <- req
							break
						}
					}
				}
*/
/*func removeConSlice(s []*consub, i int) []*consub {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}*/

func makeMatcher(s string) *regexp.Regexp {
	spl := strings.Split(s, ".")
	var result string
	for _, v := range spl {
		switch v {
		case "#":
			result += `.*\.`
		case "*":
			result += `\w+\.`
		default:
			result += v + `\.`
		}
	}
	//result = strings.TrimPrefix(result, `\.`)
	result = strings.TrimSuffix(result, `\.`)
	result = "^" + result + "$"
	//fmt.Println("new matcher", s, result)
	var expr = regexp.MustCompile(result)
	return expr
}

func getMatcher() func(string) *regexp.Regexp {
	matchers := make(map[string]*regexp.Regexp)
	var mux sync.Mutex
	return func(s string) *regexp.Regexp {
		mux.Lock()
		elem, ok := matchers[s]
		mux.Unlock()
		if ok {
			return elem
		} else {
			newMatcher := makeMatcher(s)
			mux.Lock()
			matchers[s] = newMatcher
			mux.Unlock()
			return newMatcher
		}
	}
}

func main() {
	//runtime.SetBlockProfileRate(1)
	//go printStats()
	fmt.Println("numcpu", runtime.NumCPU())
	fmt.Println("gomaxprocs", runtime.GOMAXPROCS(-1))
	/*runtime.GOMAXPROCS(256)
	fmt.Println("gomaxprocs", runtime.GOMAXPROCS(-1))*/

	port := ":8080"
	/*go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()*/

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create grpc server
	s := grpc.NewServer()
	pb.RegisterMessageBrokerServer(s, server{})

	fmt.Println("Server started successfully at port ", port)

	for i := 0; i < 1000; i++ {
		go processMsgQueue()
	}
	// and start...
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
