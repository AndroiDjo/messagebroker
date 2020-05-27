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

type consuMsg struct {
	key     string
	payload []byte
}

type consumer struct {
	subs     map[string]*regexp.Regexp
	msgqueue []consuMsg
	mux      sync.Mutex
}

var matcherfunc func(string) *regexp.Regexp = getMatcher()
var consumeRegister []*consumer = make([]*consumer, 10)

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

		if len(req.Key) > 0 {
			for _, c := range consumeRegister {
				if c != nil {
					c.mux.Lock()
					for _, element := range c.subs {
						if element != nil {
							if element.MatchString(req.Key) {
								c.msgqueue = append(c.msgqueue, consuMsg{key: req.Key, payload: req.Payload})
								break
							}
						}
					}
					c.mux.Unlock()
				}
			}
		}
	}
}

func (s server) Consume(srv pb.MessageBroker_ConsumeServer) error {
	ctx := srv.Context()
	cons := consumer{subs: make(map[string]*regexp.Regexp)}
	consumeRegister = append(consumeRegister, &cons)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		cons.mux.Lock()
		for _, msg := range cons.msgqueue {
			resp := pb.ConsumeResponse{Key: msg.key, Payload: msg.payload}
			if err := srv.Send(&resp); err != nil {
				log.Printf("Consume send error %v", err)
			}
		}
		cons.msgqueue = cons.msgqueue[:0]
		cons.mux.Unlock()

		req, err := srv.Recv()
		if err == io.EOF {
			continue
		}
		if err != nil {
			log.Printf("Consume receive error %v", err)
			continue
		}

		if len(req.Keys) > 0 {
			cons.mux.Lock()
			if req.Action == pb.ConsumeRequest_SUBSCRIBE {
				for _, key := range req.Keys {
					cons.subs[key] = matcherfunc(key)
				}
			} else if req.Action == pb.ConsumeRequest_UNSUBSCRIBE {
				for _, key := range req.Keys {
					cons.subs[key] = nil
				}
			}
			cons.mux.Unlock()
		}
	}
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
	var expr = regexp.MustCompile(result)
	return expr
}

func getMatcher() func(string) *regexp.Regexp {
	matchers := make(map[string]*regexp.Regexp)
	return func(s string) *regexp.Regexp {
		elem, ok := matchers[s]
		if ok {
			return elem
		} else {
			newMatcher := makeMatcher(s)
			matchers[s] = newMatcher
			return newMatcher
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", ":80")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create grpc server
	s := grpc.NewServer()
	pb.RegisterMessageBrokerServer(s, server{})

	fmt.Println("Server started successfully")

	// and start...
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
