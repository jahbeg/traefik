package leastconnection

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/traefik/traefik/v2/pkg/healthcheck"
	"github.com/traefik/traefik/v2/pkg/log"
	rr "github.com/vulcand/oxy/roundrobin"
	"github.com/vulcand/oxy/utils"
)

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

// New creates a new LeastConnection.
func New(next http.Handler, stickySession *rr.StickySession, rrl rr.RequestRewriteListener, errHdl utils.ErrorHandler) (healthcheck.BalancerHandler, error) {
	lc := &LeastConnection{
		next:                   next,
		index:                  -1,
		mutex:                  &sync.Mutex{},
		servers:                []*server{},
		stickySession:          stickySession,
		requestRewriteListener: rrl,
		errHandler:             errHdl,
	}

	if lc.errHandler == nil {
		lc.errHandler = utils.DefaultHandler
	}
	return lc, nil
}

func (r *LeastConnection) decrementAmountOfConnections(req *http.Request) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if req.URL == nil {
		return fmt.Errorf("leastconnection: invalid state request does not have uri")
	}

	s, index := r.findServerByURL(req.URL)
	if s == nil || index == -1 {
		return fmt.Errorf("leastconnection: invalid state, server is nil or server %s does not exist", req.URL)
	}

	if s.amountConnections == 0 {
		return fmt.Errorf("leastconnection: invalid state, server %v already has 0 connection", req.URL)
	}

	s.amountConnections--
	return nil
}

func (r *LeastConnection) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// make shallow copy of request before chaining anything to avoid side effects
	newReq := *req
	defer func() {
		if e := r.decrementAmountOfConnections(&newReq); e != nil {
			log.WithoutContext().Errorf("leastconnection: could not decrement connections %v", e)
		}

		log.WithoutContext().Infof("leastconnection: completed ServeHttp on request")
	}()

	stuck := false
	if r.stickySession != nil {
		cookieURL, present, err := r.stickySession.GetBackend(&newReq, r.Servers())
		if err != nil {
			log.WithoutContext().Warnf("leastconnection: error using server from cookie: %v", err)
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

	log.WithoutContext().Debugf("leastconnection: Forwarding this request to URL")

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
	log.WithoutContext().Infof("leastconnection: chose server %s with minimal amount of connections (%d)", chosenServer.url, chosenServer.amountConnections)
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
func (r *LeastConnection) UpsertServer(u *url.URL, options ...rr.ServerOption) error {
	// server options are useless since we do not care about the weight

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if u == nil {
		return fmt.Errorf("server URL can't be nil")
	}

	// if already exists shortcuts
	if s, _ := r.findServerByURL(u); s != nil {
		return nil
	}

	srv := &server{url: utils.CopyURL(u)}
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

// Set additional parameters for the server can be supplied when adding server.
type server struct {
	url *url.URL
	// Relative weight for the enpoint to other enpoints in the load balancer
	amountConnections int
}

func sameURL(a, b *url.URL) bool {
	return a.Path == b.Path && a.Host == b.Host && a.Scheme == b.Scheme
}
