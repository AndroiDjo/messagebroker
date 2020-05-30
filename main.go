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
	subs  []*consub
	queue chan *pb.ProduceRequest
}

var msgQueueChan chan *pb.ProduceRequest = make(chan *pb.ProduceRequest, 10000)
var consRegistry []*consumer
var produceCnt int = 0
var consumeCnt int = 0

//var subsRegistry []*consub

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
	}
}

/*func clearUnusedSubs() {
	for {
		time.Sleep(time.Second * 5)
		templcnt := 0
		maxsubs := 0
		unusedcnt := 0
		for i, v := range subsRegistry {
			if v != nil {
				if v.matcher != nil {
					templcnt++
				}
			}
			found := false
			subscnt := 0
			for _, cons := range consRegistry {
				if cons != nil {
					for _, sub := range cons.subs {
						if v == sub {
							subscnt++
							found = true
							break
						}
					}
					if found {
						break
					}
				}
			}
			if subscnt > maxsubs {
				maxsubs = subscnt
			}
			if !found {
				unusedcnt++
				subsRegistry = removeConSlice(subsRegistry, i)
			}
		}
		fmt.Println("subscription stats:", len(subsRegistry), templcnt, maxsubs, unusedcnt)
		PrintMemUsage()
	}
}*/

func (s server) Produce(srv pb.MessageBroker_ProduceServer) error {
	ctx := srv.Context()
	produceCnt++
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

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

		/*go func() {
			for _, sub := range subsRegistry {
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
						for _, c := range sub.msgchan {
							r := myrequest{key: req.Key, payload: req.Payload}
							c <- &r
						}
					}
				}
			}
		}()*/
		/*go func(rq *pb.ProduceRequest) {
			for _, cons := range consRegistry {
				for _, sub := range cons.subs {
					if sub != nil {
						match := false
						if sub.matcher != nil {
							if sub.matcher.MatchString(rq.Key) {
								match = true
							}
						} else if sub.key == rq.Key {
							match = true
						}
						if match {
							r := myrequest{key: rq.Key, payload: rq.Payload}
							cons.queue <- &r
							break
						}
					}
				}
			}
		}(req)*/
	}
}

func (s server) Consume(srv pb.MessageBroker_ConsumeServer) error {
	ctx := srv.Context()
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
				continue
			}
			if err != nil {
				//log.Printf("Consumer request error %v", err)
				closechan <- true
				return
			}

			switch req.Action {
			case pb.ConsumeRequest_SUBSCRIBE:
				for i, _ := range req.Keys {
					found := false
					for _, s := range cons.subs {
						if req.Keys[i] == s.key {
							found = true
							break
						}
					}
					if !found {
						sub := consub{key: req.Keys[i]}
						if strings.Contains(req.Keys[i], "*") || strings.Contains(req.Keys[i], "#") {
							sub.matcher = makeMatcher(req.Keys[i])
						}
						cons.subs = append(cons.subs, &sub)
					}
				}
			case pb.ConsumeRequest_UNSUBSCRIBE:
				for idx, _ := range req.Keys {
					for i, s := range cons.subs {
						if req.Keys[idx] == s.key {
							cons.subs = removeConSlice(cons.subs, i)
						}
					}
				}
			}
		}
	}()
	/*go func() {
		for {
			req, err := srv.Recv()
			if err == io.EOF {
				continue
			}
			if err != nil {
				log.Printf("Consumer request error %v", err)
				closechan <- true
				return
			}

			switch req.Action {
			case pb.ConsumeRequest_SUBSCRIBE:
				for _, key := range req.Keys {
					found := false
					for _, s := range cons.subs {
						if key == s.key {
							found = true
							break
						}
					}
					if !found {
						sub := consub{key: key}
						if strings.Contains(key, "*") || strings.Contains(key, "#") {
							sub.matcher = makeMatcher(key)
						}
						cons.subs = append(cons.subs, &sub)
					}
				}
			case pb.ConsumeRequest_UNSUBSCRIBE:
				for _, key := range req.Keys {
					for i, s := range cons.subs {
						if key == s.key {
							//fmt.Println("remove matcher:", key)
							lk := len(cons.subs)
							cons.subs[i], cons.subs = cons.subs[lk-1], cons.subs[:lk-1]
						}
					}
				}
			}
		}
	}()*/

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-exitchan:
		return nil
	}
}

func processMsgQueue() {
	for {
		req := <-msgQueueChan
		for _, cons := range consRegistry {
			if cons != nil {
				for _, sub := range cons.subs {
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
			}
		}
	}
}

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

	port := ":80"
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
