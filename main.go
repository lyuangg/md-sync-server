// md-sync-server relays editor scroll positions to preview pages over SSE.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const protoVersion = 1

// Message is the shared wire format.
type Message struct {
	V       int    `json:"v"`
	Type    string `json:"type"`
	Session string `json:"session"`
	File    string `json:"file,omitempty"`
	Line    int    `json:"line,omitempty"`
	Seq     uint64 `json:"seq,omitempty"`
}

type subscriber struct {
	session string
	ch      chan Message
}

type hub struct {
	mu      sync.Mutex
	subs    map[*subscriber]struct{}
	last    map[string]Message
	seq     uint64
	onCount func(session string, n int)
	done    chan struct{}
	once    sync.Once
}

func newHub(onCount func(string, int)) *hub {
	return &hub{
		subs:    make(map[*subscriber]struct{}),
		last:    make(map[string]Message),
		onCount: onCount,
		done:    make(chan struct{}),
	}
}

func (h *hub) stop() {
	h.once.Do(func() { close(h.done) })
}

func (h *hub) countLocked(session string) int {
	n := 0
	for x := range h.subs {
		if x.session == session {
			n++
		}
	}
	return n
}

func (h *hub) add(s *subscriber) {
	h.mu.Lock()
	h.subs[s] = struct{}{}
	n := h.countLocked(s.session)
	h.mu.Unlock()
	if h.onCount != nil {
		h.onCount(s.session, n)
	}
}

func (h *hub) remove(s *subscriber) {
	h.mu.Lock()
	delete(h.subs, s)
	n := h.countLocked(s.session)
	h.mu.Unlock()
	if h.onCount != nil {
		h.onCount(s.session, n)
	}
}

func (h *hub) lastMsg(session string) (Message, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.last[session]
	return m, ok
}

// broadcastAll is used to close every preview tab when the editor exits.
func (h *hub) broadcastAll(m Message) {
	h.mu.Lock()
	h.seq++
	m.Seq = h.seq
	targets := make([]*subscriber, 0, len(h.subs))
	for s := range h.subs {
		targets = append(targets, s)
	}
	h.mu.Unlock()
	for _, s := range targets {
		select {
		case s.ch <- m:
		default:
		}
	}
}

func (h *hub) eof(closeOnExit bool) {
	if closeOnExit {
		h.broadcastAll(Message{V: protoVersion, Type: "close"})
		// Wait briefly so the final SSE frame reaches the browser.
		time.Sleep(200 * time.Millisecond)
	}
	h.stop()
}

func (h *hub) broadcast(m Message) {
	h.mu.Lock()
	h.seq++
	m.Seq = h.seq
	if m.Type == "pos" {
		h.last[m.Session] = m
	}
	targets := make([]*subscriber, 0, len(h.subs))
	for s := range h.subs {
		if s.session == m.Session {
			targets = append(targets, s)
		}
	}
	h.mu.Unlock()

	for _, s := range targets {
		select {
		case s.ch <- m:
		default:
		}
	}
}

func serveEvents(h *hub, token string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.URL.Query().Get("token")), []byte(token)) != 1 {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	session := r.URL.Query().Get("session")
	if !validSession(session) {
		http.Error(w, "bad session", http.StatusBadRequest)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "retry: 1000\n\n")
	fl.Flush()

	sub := &subscriber{session: session, ch: make(chan Message, 64)}
	h.add(sub)
	defer h.remove(sub)

	if last, ok := h.lastMsg(session); ok {
		if writeEvent(w, fl, last) != nil {
			return
		}
	}

	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-h.done:
			return
		case <-ping.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		case m := <-sub.ch:
			if writeEvent(w, fl, m) != nil {
				return
			}
		}
	}
}

func writeEvent(w io.Writer, fl http.Flusher, m Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", m.Seq, b); err != nil {
		return err
	}
	fl.Flush()
	return nil
}

func validSession(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '_', r == ':', r == '-':
		default:
			return false
		}
	}
	return true
}

func readStdin(h *hub, closeOnExit bool) {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var m Message
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			log.Printf("md-sync: bad message: %v", err)
			continue
		}
		// "close" does not carry a session; it targets all tabs.
		if m.Type == "close" {
			h.broadcastAll(Message{V: protoVersion, Type: "close"})
			continue
		}
		if !validSession(m.Session) {
			log.Printf("md-sync: bad session %q", m.Session)
			continue
		}
		switch m.Type {
		case "pos", "reload", "focus":
			m.V = protoVersion
			h.broadcast(m)
		default:
			log.Printf("md-sync: unknown type %q", m.Type)
		}
	}
	h.eof(closeOnExit) // stdin EOF means the editor exited
}

func main() {
	base := flag.Int("port-base", 8765, "first port to try")
	tries := flag.Int("tries", 10, "how many consecutive ports to try")
	closeOnExit := flag.Bool("close-on-exit", true, "ask preview tabs to close when the editor exits")
	flag.Parse()

	log.SetFlags(0)
	log.SetOutput(os.Stderr)

	ln, port, err := listen(*base, *tries)
	if err != nil {
		log.Printf("md-sync: %v", err)
		os.Exit(1)
	}
	token := newToken()

	var outMu sync.Mutex
	emit := func(v any) {
		outMu.Lock()
		defer outMu.Unlock()
		_ = json.NewEncoder(os.Stdout).Encode(v)
	}
	emit(map[string]any{
		"v": protoVersion, "type": "hello",
		"port": port, "token": token, "pid": os.Getpid(),
	})

	h := newHub(func(session string, n int) {
		emit(map[string]any{"v": protoVersion, "type": "client", "session": session, "count": n})
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(map[string]any{"app": "md-sync", "v": protoVersion})
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		serveEvents(h, token, w, r)
	})

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// Do not set WriteTimeout: SSE streams are long-lived.
	}

	var exiting atomic.Bool
	shutdown := func() {
		if !exiting.CompareAndSwap(false, true) {
			return
		}
		h.stop()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("md-sync: serve: %v", err)
			shutdown()
		}
	}()
	go readStdin(h, *closeOnExit)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sig:
		case <-h.done:
		}
		shutdown()
	}()

	<-h.done
	shutdown()
}

func listen(base, tries int) (net.Listener, int, error) {
	if base <= 0 || base > 65535 || tries <= 0 {
		return nil, 0, fmt.Errorf("bad port config %d/%d", base, tries)
	}
	var lastErr error
	// Try consecutive ports so multiple editors can coexist.
	for i := 0; i < tries && base+i <= 65535; i++ {
		port := base + i
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			return ln, port, nil
		}
		lastErr = err
	}
	return nil, 0, fmt.Errorf("no free port in %d..%d: %w", base, base+tries-1, lastErr)
}

func newToken() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
