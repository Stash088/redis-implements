package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// replTimeout bounds every step of the handshake and every read of the
// stream: a master that stays silent for this long is considered lost and
// the link reconnects (the repl-timeout of Redis).
const replTimeout = 30 * time.Second

// replRetryDelay is the pause before a replica reconnects to its master.
const replRetryDelay = time.Second

// replAckPeriod is how often the replica reports its offset to the master
// (REPLCONF ACK), so WAIT on the master does not wait longer than necessary.
const replAckPeriod = time.Second

// replicaClient maintains the outgoing link of this server (as a replica) to
// its master: the handshake, the full synchronization and the command stream.
// On every loss of the master it reconnects and tries a partial resync
// first, falling back to the full one.
type replicaClient struct {
	srv  *Server
	host string
	port int
	auth string

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}

	// writeMu serializes the writes to the master connection: the stream
	// reader answers REPLCONF GETACK and the heartbeat writes ACKs of its
	// own.
	writeMu sync.Mutex

	// conn is the current connection: stopping closes it to unblock a read
	// in progress.
	mu   sync.Mutex
	conn net.Conn

	// id and off are the replication ID and offset of the last
	// synchronization: the arguments of the PSYNC of a reconnection.
	id  string
	off int64
}

func newReplicaClient(s *Server, host string, port int, auth string) *replicaClient {
	return &replicaClient{
		srv:  s,
		host: host,
		port: port,
		auth: auth,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (rc *replicaClient) start() { go rc.run() }

// stopAndWait asks the link to finish and waits for it. Closing the current
// connection unblocks a read in progress.
func (rc *replicaClient) stopAndWait() {
	rc.stopOnce.Do(func() { close(rc.stop) })
	rc.mu.Lock()
	conn := rc.conn
	rc.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	<-rc.done
}

// stopped reports whether the link was asked to finish.
func (rc *replicaClient) stopped() bool {
	select {
	case <-rc.stop:
		return true
	default:
		return false
	}
}

func (rc *replicaClient) masterAddr() string {
	return net.JoinHostPort(rc.host, strconv.Itoa(rc.port))
}

// run keeps the link alive: connect, sync, stream; on a loss — reconnect.
func (rc *replicaClient) run() {
	defer close(rc.done)
	for {
		if rc.stopped() {
			return
		}
		rc.setLinkState(linkConnect)
		err := rc.syncOnce()
		if rc.stopped() {
			return
		}
		if err != nil {
			rc.srv.Logger.Printf("replica: sync with %s failed: %v", rc.masterAddr(), err)
		}
		rc.setLinkState(linkDown)
		select {
		case <-rc.stop:
			return
		case <-time.After(replRetryDelay):
		}
	}
}

// setLinkState records the state of the link for ROLE and INFO. A link that
// was replaced (REPLICAOF to another master) leaves the state alone.
func (rc *replicaClient) setLinkState(st linkState) {
	r := rc.srv.repl
	r.mu.Lock()
	cur := r.link
	r.mu.Unlock()
	if cur == rc {
		r.state.Store(int32(st))
	}
}

// setStreamID adopts the replication ID and offset the master reported: the
// replica inherits the history of its master.
func (rc *replicaClient) setStreamID(id string, off int64) {
	r := rc.srv.repl
	r.mu.Lock()
	r.id = id
	r.offset.Store(off)
	r.mu.Unlock()
	rc.id, rc.off = id, off
}

// syncOnce performs one handshake, synchronization and stream session. It
// returns when the master is lost.
func (rc *replicaClient) syncOnce() error {
	conn, err := net.DialTimeout("tcp", rc.masterAddr(), 5*time.Second)
	if err != nil {
		return err
	}
	rc.mu.Lock()
	rc.conn = conn
	rc.mu.Unlock()
	defer func() {
		conn.Close()
		rc.mu.Lock()
		rc.conn = nil
		rc.mu.Unlock()
	}()

	br := bufio.NewReaderSize(conn, resp.DefaultReadBufferSize)
	bw := bufio.NewWriterSize(conn, resp.DefaultWriteBufferSize)
	touchIO := func() {
		conn.SetReadDeadline(time.Now().Add(replTimeout)) //nolint:errcheck
		rc.srv.repl.lastIO.Store(time.Now().Unix())
	}
	touchIO()

	// AUTH (when the master requires a password), PING, REPLCONF, PSYNC.
	// Every step expects a status reply; an error reply fails the step (the
	// errors of REPLCONF are tolerated: not every master knows every
	// option, like Redis tolerates them).
	if rc.auth != "" {
		if err := rc.statusReply(br, bw, touchIO, "auth", rc.auth); err != nil {
			return err
		}
	}
	if err := rc.statusReply(br, bw, touchIO, "ping"); err != nil {
		return err
	}
	if err := rc.replconf(br, bw, touchIO, "listening-port", strconv.Itoa(rc.srv.port())); err != nil {
		return err
	}
	// psync2 only: the RDB arrives with a length prefix, so the eof
	// capability is not announced.
	if err := rc.replconf(br, bw, touchIO, "capa", "psync2"); err != nil {
		return err
	}
	psyncID, psyncOff := rc.id, rc.off
	if err := rc.send(bw, "psync", psyncID, strconv.FormatInt(psyncOff, 10)); err != nil {
		return err
	}
	touchIO()
	line, err := readLine(br)
	if err != nil {
		return err
	}
	rc.srv.repl.lastIO.Store(time.Now().Unix())
	rc.setLinkState(linkSync)

	switch {
	case strings.HasPrefix(line, "+FULLRESYNC"):
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return fmt.Errorf("bad +FULLRESYNC reply %q", line)
		}
		off, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("bad +FULLRESYNC offset %q", fields[2])
		}
		rc.setStreamID(fields[1], off)
		if err := rc.receiveRDB(br); err != nil {
			return err
		}
	case strings.HasPrefix(line, "+CONTINUE"):
		// The master kept our history: the stream continues from the offset
		// we asked for. A renewed replication ID may come with the reply.
		fields := strings.Fields(line)
		if len(fields) == 2 {
			rc.setStreamID(fields[1], rc.off)
		}
	default:
		// The master refused the PSYNC: it does not know our history. The
		// next attempt asks for a full synchronization.
		rc.id, rc.off = "?", -1
		return fmt.Errorf("master refused psync %s %d: %s", psyncID, psyncOff, line)
	}
	rc.setLinkState(linkUp)

	// The command stream: apply until the master is lost. The heartbeat
	// reports our offset once a second, so WAIT on the master does not wait
	// longer than necessary.
	ackStop := make(chan struct{})
	go rc.heartbeat(ackStop)
	err = rc.stream(br, touchIO)
	close(ackStop)
	return err
}

// send writes a command to the master: the array header first, then the bulk
// strings.
func (rc *replicaClient) send(w *bufio.Writer, args ...string) error {
	rc.writeMu.Lock()
	defer rc.writeMu.Unlock()
	if _, err := w.WriteString("*" + strconv.Itoa(len(args)) + "\r\n"); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := w.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n"); err != nil {
			return err
		}
	}
	return w.Flush()
}

// statusReply sends a command and reads the status reply. An error reply
// fails the step.
func (rc *replicaClient) statusReply(br *bufio.Reader, bw *bufio.Writer, touchIO func(), args ...string) error {
	if err := rc.send(bw, args...); err != nil {
		return err
	}
	touchIO()
	line, err := readLine(br)
	if err != nil {
		return err
	}
	rc.srv.repl.lastIO.Store(time.Now().Unix())
	if strings.HasPrefix(line, "-") {
		return fmt.Errorf("%s rejected: %s", args[0], line[1:])
	}
	return nil
}

// replconf is a statusReply step whose error is tolerated: not every master
// knows every REPLCONF option, like Redis tolerates the rejection.
func (rc *replicaClient) replconf(br *bufio.Reader, bw *bufio.Writer, touchIO func(), args ...string) error {
	if err := rc.send(bw, append([]string{"replconf"}, args...)...); err != nil {
		return err
	}
	touchIO()
	line, err := readLine(br)
	if err != nil {
		return err
	}
	rc.srv.repl.lastIO.Store(time.Now().Unix())
	if strings.HasPrefix(line, "-") {
		rc.srv.Logger.Printf("replica: (non-critical) master does not understand REPLCONF %s: %s", args[0], line[1:])
	}
	return nil
}

// receiveRDB reads the bulk RDB of the full synchronization and loads it.
// The replica does not announce the eof capability, so the master sends
// "$<len>\r\n" followed by exactly len bytes (no trailing CRLF, like Redis
// reads it).
func (rc *replicaClient) receiveRDB(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "$") {
		return fmt.Errorf("bad RDB preamble %q", line)
	}
	n, err := strconv.ParseInt(line[1:], 10, 64)
	if err != nil || n < 0 {
		return fmt.Errorf("bad RDB length %q", line)
	}
	rdb := make([]byte, n)
	if _, err := io.ReadFull(br, rdb); err != nil {
		return fmt.Errorf("rdb transfer: %w", err)
	}

	s := rc.srv
	// The dataset of the master replaces ours: the local keys are dropped
	// right before the load.
	for _, db := range s.dbsList() {
		db.Flush()
	}
	if err := persist.LoadBytes(rdb, s.dbsList()); err != nil {
		return fmt.Errorf("rdb load: %w", err)
	}
	return nil
}

// stream reads the commands of the master and applies them to the local
// stores. It returns when the master is lost.
func (rc *replicaClient) stream(br *bufio.Reader, touchIO func()) error {
	s := rc.srv
	ap := persist.NewApplier(s.dbsList())
	// SWAPDB of the master reaches the slice of the databases through the
	// server: the slice is replaced atomically for the readers, and the
	// applier continues with the slice that owns the result.
	ap.SetSwapDBs(func(stores []*store.Store, a, b int) []*store.Store {
		s.SwapDBs(a, b)
		return s.dbsList()
	})
	rd := resp.NewReaderSize(br, resp.DefaultReadBufferSize)
	for {
		touchIO()
		v, err := rd.ReadValue()
		if err != nil {
			return err
		}
		s.repl.lastIO.Store(time.Now().Unix())
		if v.Kind != resp.KindArray || v.IsNull || len(v.Array) == 0 {
			return errors.New("bad command frame in the replication stream")
		}
		args := make([][]byte, 0, len(v.Array))
		for i := range v.Array {
			el := v.Array[i]
			if el.Kind != resp.KindBulkString || el.IsNull {
				return errors.New("bad argument in the replication stream")
			}
			args = append(args, el.Str)
		}
		// The offset counts every byte of the stream, the protocol traffic
		// included: the master counted the same bytes when it fed it.
		rc.srv.repl.offset.Add(int64(len(persist.EncodeCommand(args))))
		switch strings.ToLower(string(args[0])) {
		case "ping":
			// The heartbeat of the master: nothing to apply.
		case "replconf":
			// REPLCONF GETACK: the master asks for our offset (WAIT).
			if len(args) >= 2 && strings.EqualFold(string(args[1]), "getack") {
				rc.sendAck()
			}
		default:
			if err := ap.Apply(args); err != nil {
				return fmt.Errorf("apply %s: %w", args[0], err)
			}
		}
	}
}

// sendAck replies REPLCONF ACK <offset> to the master.
func (rc *replicaClient) sendAck() {
	off := rc.srv.repl.offset.Load()
	rc.mu.Lock()
	conn := rc.conn
	rc.mu.Unlock()
	if conn == nil {
		return
	}
	bw := bufio.NewWriter(conn)
	rc.send(bw, "replconf", "ack", strconv.FormatInt(off, 10)) //nolint:errcheck
}

// heartbeat reports the offset to the master once a second.
func (rc *replicaClient) heartbeat(stop chan struct{}) {
	t := time.NewTicker(replAckPeriod)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-rc.stop:
			return
		case <-t.C:
			rc.sendAck()
		}
	}
}

// readLine reads one CRLF-terminated line from the buffered reader.
func readLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}
