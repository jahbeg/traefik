package leastconnections

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/traefik/traefik/v2/pkg/log"
	rr "github.com/vulcand/oxy/roundrobin"
	"github.com/vulcand/oxy/utils"
)

// ErrorHandler is a functional argument that sets error handler of the server.
func ErrorHandler(h utils.ErrorHandler) LBOption {
	return func(s *LeastConnection) error {
		s.errHandler = h
		return nil
	}
}

// EnableStickySession enable sticky session.
func EnableStickySession(stickySession *rr.StickySession) LBOption {
	return func(s *LeastConnection) error {
		s.stickySession = stickySession
		return nil
	}
}

// LeastConnectionRequestRewriteListener is a functional argument that sets error handler of the server.
func LeastConnectionRequestRewriteListener(rrl rr.RequestRewriteListener) LBOption {
	return func(s *LeastConnection) error {
		s.requestRewriteListener = rrl
		return nil
	}
}

// LeastConnection implements a least connection
// load balancing strategy
type LeastConnection struct {
	mutex      *sync.Mutex
	next       http.Handler
	errHandler utils.ErrorHandler
	// Current index (starts from -1)
	index                  int
	servers                []*server
	stickySession          *rr.StickySession
	requestRewriteListener rr.RequestRewriteListener
}

// New created a new LeastConnection.
func New(next http.Handler, opts ...LBOption) (*LeastConnection, error) {
	rr := &LeastConnection{
		next:          next,
		index:         -1,
		mutex:         &sync.Mutex{},
		servers:       []*server{},
		stickySession: nil,
	}
	for _, o := range opts {
		if err := o(rr); err != nil {
			return nil, err
		}
	}
	if rr.errHandler == nil {
		rr.errHandler = utils.DefaultHandler
	}
	return rr, nil
}

func (r *LeastConnection) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer log.WithoutContext().Debugf("vulcand/oxy/roundrobin/rr: completed ServeHttp on request")

	// make shallow copy of request before chaining anything to avoid side effects
	newReq := *req
	stuck := false
	if r.stickySession != nil {
		cookieURL, present, err := r.stickySession.GetBackend(&newReq, r.Servers())
		if err != nil {
			log.WithoutContext().Warnf("vulcand/oxy/roundrobin/rr: error using server from cookie: %v", err)
		}

		if present {
			newReq.URL = cookieURL
			stuck = true
		}
	}

	if !stuck {
		uri, err := r.NextServer()
		if err != nil {
			r.errHandler.ServeHTTP(w, req, err)
			return
		}

		if r.stickySession != nil {
			r.stickySession.StickBackend(uri, w)
		}
		newReq.URL = uri
	}

	log.WithoutContext().Debugf("vulcand/oxy/roundrobin/rr: Forwarding this request to URL")

	// Emit event to a listener if one exists
	if r.requestRewriteListener != nil {
		r.requestRewriteListener(req, &newReq)
	}

	r.next.ServeHTTP(w, &newReq)
}

// NextServer gets the next server.
func (r *LeastConnection) NextServer() (*url.URL, error) {
	srv, err := r.nextServer()
	if err != nil {
		return nil, err
	}
	return utils.CopyURL(srv.url), nil
}

func (r *LeastConnection) nextServer() (*server, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.servers) == 0 {
		return nil, fmt.Errorf("no servers in the pool")
	}

	// check the min amount of connections
	idx := 0
	min := -1
	for i, s := range r.servers {
		if min == -1 || s.amountConnections < min {
			min = s.amountConnections
			idx = i
		}
	}

	// choose the server
	chosenServer := r.servers[idx]
	chosenServer.amountConnections++

	return chosenServer, nil
}

// RemoveServer remove a server.
func (r *LeastConnection) RemoveServer(u *url.URL) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	e, index := r.findServerByURL(u)
	if e == nil {
		return fmt.Errorf("server not found")
	}
	r.servers = append(r.servers[:index], r.servers[index+1:]...)
	return nil
}

// Servers gets servers URL.
func (r *LeastConnection) Servers() []*url.URL {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	out := make([]*url.URL, len(r.servers))
	for i, srv := range r.servers {
		out[i] = srv.url
	}
	return out
}

// UpsertServer In case if server is already present in the load balancer, returns error.
func (r *LeastConnection) UpsertServer(u *url.URL, options ...ServerOption) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if u == nil {
		return fmt.Errorf("server URL can't be nil")
	}

	// if already exists shortcuts
	if s, _ := r.findServerByURL(u); s != nil {
		for _, o := range options {
			if err := o(s); err != nil {
				return err
			}
		}
		return nil
	}

	srv := &server{url: utils.CopyURL(u)}
	for _, o := range options {
		if err := o(srv); err != nil {
			return err
		}
	}

	r.servers = append(r.servers, srv)
	return nil
}

func (r *LeastConnection) findServerByURL(u *url.URL) (*server, int) {
	if len(r.servers) == 0 {
		return nil, -1
	}
	for i, s := range r.servers {
		if sameURL(u, s.url) {
			return s, i
		}
	}
	return nil, -1
}

// ServerOption provides various options for server, e.g. weight.
type ServerOption func(*server) error

// LBOption provides options for load balancer.
type LBOption func(*LeastConnection) error

// Set additional parameters for the server can be supplied when adding server.
type server struct {
	url *url.URL
	// Relative weight for the enpoint to other enpoints in the load balancer
	amountConnections int
}

func sameURL(a, b *url.URL) bool {
	return a.Path == b.Path && a.Host == b.Host && a.Scheme == b.Scheme
}

type balancerHandler interface {
	Servers() []*url.URL
	ServeHTTP(w http.ResponseWriter, req *http.Request)
	ServerWeight(u *url.URL) (int, bool)
	RemoveServer(u *url.URL) error
	UpsertServer(u *url.URL, options ...ServerOption) error
	NextServer() (*url.URL, error)
	Next() http.Handler
}
