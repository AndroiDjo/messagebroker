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

	pb "github.com/AndroiDjo/newbroker/mbproto"
	"google.golang.org/grpc"
)

type server struct{}

type consub struct {
	key     string
	matcher *regexp.Regexp
}

type consumer struct {
	//subs  []*consub
	subkeys  []string
	subtstar []*consub
	subtcell []*consub
	queue    chan *pb.ProduceRequest
}

var msgQueueChan chan *pb.ProduceRequest = make(chan *pb.ProduceRequest, 10000)
var consRegistry []*consumer
var produceCnt int = 0
var consumeCnt int = 0

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

		go func(r *pb.ProduceRequest) {
			msgQueueChan <- r
		}(req)
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
					found := false
					for _, s := range cons.subtcell {
						if key == s.key {
							found = true
							break
						}
					}
					for _, s := range cons.subtstar {
						if key == s.key {
							found = true
							break
						}
					}
					for idx, _ := range cons.subkeys {
						if key == cons.subkeys[idx] {
							found = true
							break
						}
					}
					if !found {
						if strings.Contains(key, "#") {
							sub := consub{key: key, matcher: makeMatcher(key)}
							cons.subtcell = append(cons.subtcell, &sub)
						} else if strings.Contains(key, "*") {
							sub := consub{key: key, matcher: makeMatcher(key)}
							cons.subtstar = append(cons.subtstar, &sub)
						} else {
							cons.subkeys = append(cons.subkeys, key)
						}
					}
				}
			case pb.ConsumeRequest_UNSUBSCRIBE:
				for _, key := range req.Keys {
					removed := false
					for i, s := range cons.subtcell {
						if key == s.key {
							cons.subtcell = removeConSlice(cons.subtcell, i)
							removed = true
							break
						}
					}
					if !removed {
						for i, s := range cons.subtstar {
							if key == s.key {
								cons.subtstar = removeConSlice(cons.subtstar, i)
								removed = true
								break
							}
						}
					}
					if !removed {
						for i, _ := range cons.subkeys {
							if key == cons.subkeys[i] {
								ls := len(cons.subkeys)
								cons.subkeys[i] = cons.subkeys[ls-1]
								cons.subkeys = cons.subkeys[:ls-1]
								break
							}
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
		for _, cons := range consRegistry {
			if cons != nil {
				match := false
				for _, sub := range cons.subtcell {
					if sub != nil {
						if sub.matcher != nil {
							if sub.matcher.MatchString(req.Key) {
								match = true
								break
							}
						}
					}
				}
				if !match {
					for _, sub := range cons.subtstar {
						if sub != nil {
							if sub.matcher != nil {
								if sub.matcher.MatchString(req.Key) {
									match = true
									break
								}
							}
						}
					}
				}
				if !match {
					for _, key := range cons.subkeys {
						if key == req.Key {
							match = true
							break
						}
					}
				}
				if match {
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
func removeConSlice(s []*consub, i int) []*consub {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

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
	go printStats()
	fmt.Println("numcpu", runtime.NumCPU())
	fmt.Println("gomaxprocs", runtime.GOMAXPROCS(-1))
	/*runtime.GOMAXPROCS(256)
	fmt.Println("gomaxprocs", runtime.GOMAXPROCS(-1))*/

	port := ":80"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create grpc server
	s := grpc.NewServer()
	pb.RegisterMessageBrokerServer(s, server{})

	fmt.Println("Server started successfully at port ", port)

	for i := 0; i < 100000; i++ {
		go processMsgQueue()
	}
	// and start...
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
