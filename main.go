package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"

	pb "github.com/AndroiDjo/newbroker/mbproto"
	"google.golang.org/grpc"
)

type server struct{}

var chanlist []chan *pb.ProduceRequest
var matcherfunc func(string) *regexp.Regexp = getMatcher()

func (s server) Produce(srv pb.MessageBroker_ProduceServer) error {
	ctx := srv.Context()
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
			continue
		}

		for _, ch := range chanlist {
			ch <- req
		}
		/*if len(req.Key) > 0 {
			for _, c := range consumeRegister {
				if c != nil {
					go func(key string, pl []byte, c *consumer) {
						c.mux.Lock()
						for _, element := range c.subs {
							if element != nil {
								if element.MatchString(key) {
									//fmt.Println("produce add msg to queue:", c.idx, key)
									c.msgqueue = append(c.msgqueue, consuMsg{key: key, payload: pl})
									break
								}
							}
						}
						c.mux.Unlock()
					}(req.Key, req.Payload, c)
				}
			}
		}*/
	}
}

func (s server) Consume(srv pb.MessageBroker_ConsumeServer) error {
	ctx := srv.Context()

	var exparr []*regexp.Regexp = make([]*regexp.Regexp, 1)
	var keyarr []string = make([]string, 1)
	var msgchan chan *pb.ProduceRequest = make(chan *pb.ProduceRequest, 1000)
	chanlist = append(chanlist, msgchan)

	go func() {
		for {
			m := <-msgchan
			var ea []*regexp.Regexp
			copy(ea, exparr)
			for _, exp := range ea {
				if exp.MatchString(m.Key) {
					resp := pb.ConsumeResponse{Key: m.Key, Payload: m.Payload}
					if err := srv.Send(&resp); err != nil {
						log.Printf("Consume send error %v", err)
					}
					break
				}
			}

		}
	}()

	go func() {
		for {
			/*select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}*/

			/*for len(cons.msgqueue) > 0 {
				// TODO: try not use lock here
				cons.mux.Lock()
				cm := cons.msgqueue[0]
				cons.msgqueue = cons.msgqueue[1:]
				cons.mux.Unlock()
				go func(msg consuMsg) {
					resp := pb.ConsumeResponse{Key: msg.key, Payload: msg.payload}
					//fmt.Println("consumer recieve msg:", msg.key)
					if err := srv.Send(&resp); err != nil {
						log.Printf("Consume send error %v", err)
					}
				}(cm)
			}*/

			req, err := srv.Recv()
			if err == io.EOF || req == nil {
				continue
			}

			switch req.Action {
			case pb.ConsumeRequest_SUBSCRIBE:
				for _, key := range req.Keys {
					found := false
					for _, s := range keyarr {
						if key == s {
							found = true
							break
						}
					}
					if !found {
						expr := makeMatcher(key)
						exparr = append(exparr, expr)
						keyarr = append(keyarr, key)
					}
				}
			case pb.ConsumeRequest_UNSUBSCRIBE:
				for _, key := range req.Keys {
					for i, s := range keyarr {
						if key == s {
							lk := len(keyarr)
							keyarr[i], keyarr = keyarr[lk-1], keyarr[:lk-1]

							le := len(exparr)
							exparr[i], exparr = exparr[le-1], exparr[:le-1]
							//exparr = append(exparr[:i], exparr[i+1:]...)
						}
					}
				}
			}

			/*go func(action pb.ConsumeRequest_Action, k string, c *consumer) {
				switch action {
				case pb.ConsumeRequest_SUBSCRIBE:
					matcher := makeMatcher(k)
					c.mux.Lock()
					c.subs[k] = matcher
					c.mux.Unlock()
				case pb.ConsumeRequest_UNSUBSCRIBE:
					c.mux.Lock()
					c.subs[k] = nil
					c.mux.Unlock()
				}
			}(req.Action, key, &cons)*/

		}
	}()

	<-ctx.Done()
	return ctx.Err()
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
	port := ":80"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create grpc server
	s := grpc.NewServer()
	pb.RegisterMessageBrokerServer(s, server{})

	fmt.Println("Server started successfully at port ", port)

	// and start...
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
