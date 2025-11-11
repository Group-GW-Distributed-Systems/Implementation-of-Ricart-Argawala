package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "example.com/ra/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//
// ---------- Node / RA-state ----------
//

type State int

const (
	Released State = iota
	Wanted
	Held
)

type Node struct {
	mu    sync.Mutex
	id    int
	port  string
	peers []string

	clock    int64
	state    State
	myReqTs  int64
	replies  map[int]bool
	deferred []int

	// key: peerPort (e.g. "5002")
	clients map[string]pb.RicartagrawalaClient
}

// Trådsikre Lamport-hjælpere
func (n *Node) onSend() int64 {
	n.mu.Lock()
	n.clock++
	ts := n.clock
	n.mu.Unlock()
	return ts
}
func (n *Node) onRecv(ts int64) {
	n.mu.Lock()
	if ts > n.clock {
		n.clock = ts
	}
	n.clock++
	n.mu.Unlock()
}
func (n *Node) getClock() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.clock
}

//func (n *Node) connectToPeers() {
//	n.clients = make(map[string]pb.RicartagrawalaClient)
//	for _, peerPort := range n.peers {
//		addr := "127.0.0.1:" + peerPort
//
//		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//		conn, err := grpc.DialContext(
//			ctx,
//			addr,
//			grpc.WithTransportCredentials(insecure.NewCredentials()),
//			grpc.WithBlock(), // vent på at forbindelsen faktisk etableres
//		)
//		cancel()
//
//		if err != nil {
//			log.Printf("[DIAL_TIMEOUT] %s err=%v", addr, err)
//			continue
//		}
//		n.clients[peerPort] = pb.NewRicartagrawalaClient(conn)
//		log.Printf("[DIAL_OK] %s", addr)
//	}
//}

// Snapshot klienter indekseret af int-id (port)
func (n *Node) clientsByID() map[int]pb.RicartagrawalaClient {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[int]pb.RicartagrawalaClient, len(n.clients))
	for p, c := range n.clients {
		if id, err := strconv.Atoi(p); err == nil {
			out[id] = c
		}
	}
	return out
}

// Hjælp: sorterede keys (til logging)
func keys(m map[int]string) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// Broadcast helper (beholdt til evt. debug)
func (n *Node) SendRequestToAll(ctx context.Context, content string) {
	ts := n.onSend()
	clients := n.clientsByID()

	var wg sync.WaitGroup
	for _, cli := range clients {
		wg.Add(1)
		go func(c pb.RicartagrawalaClient) {
			defer wg.Done()
			_, err := c.Request(ctx, &pb.RequestMsg{
				SenderId:  int32(n.id),
				Timestamp: ts,
				Content:   content,
			})
			if err != nil {
				log.Printf("[SEND_FAIL] request err=%v", err)
			} else {
				log.Printf("[SEND_OK] request ts=%d", ts)
			}
		}(cli)
	}
	wg.Wait()
}

// Reply til én peerPort (lazy dial)
func (n *Node) SendReplyTo(ctx context.Context, peerPort string, content string) {
	cli := n.getOrDialClient(peerPort)
	if cli == nil {
		log.Printf("reply -> %s skipped (no client)", peerPort)
		return
	}
	ts := n.onSend()
	_, err := cli.Reply(ctx, &pb.ReplyMsg{
		SenderId:  int32(n.id),
		Timestamp: ts,
		Content:   content,
	})
	if err != nil {
		log.Printf("reply -> %s failed: %v", peerPort, err)
	}
}

// RA: ind i CS (broadcast + vent på replies). Returnerer true hvis vi fik adgang.
// Vent på alle konfigurerede peers (n.peers) og lazy-dial ved send.
func (n *Node) EnterCS(ctx context.Context) bool {
	n.mu.Lock()
	n.state = Wanted
	n.myReqTs = n.clock + 1
	n.clock = n.myReqTs
	n.replies = make(map[int]bool)
	myID, myTs := n.id, n.myReqTs

	// Ventemængde = alle deklarerede peers
	peerIDs := make([]int, 0, len(n.peers))
	peerPorts := make(map[int]string, len(n.peers))
	for _, p := range n.peers {
		if id, err := strconv.Atoi(p); err == nil {
			peerIDs = append(peerIDs, id)
			peerPorts[id] = p
		}
	}
	n.mu.Unlock()

	log.Printf("[REQ] node=%d ts=%d broadcast to %v", myID, myTs, peerIDs)

	// Broadcast til ALLE peers (lazy dial per peer)
	var wg sync.WaitGroup
	for _, pid := range peerIDs {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			port := peerPorts[pid]
			cli := n.getOrDialClient(port)
			if cli == nil {
				log.Printf("[SEND_FAIL] req -> %d (no client)", pid)
				return
			}
			_, err := cli.Request(ctx, &pb.RequestMsg{
				SenderId:  int32(myID),
				Timestamp: myTs,
				Content:   "req",
			})
			if err != nil {
				log.Printf("[SEND_FAIL] req -> %d err=%v", pid, err)
			} else {
				log.Printf("[SEND_OK] req -> %d", pid)
			}
		}(pid)
	}
	wg.Wait()

	// ingen peers → gå direkte i Held
	if len(peerIDs) == 0 {
		n.mu.Lock()
		n.state = Held
		n.mu.Unlock()
		return true
	}

	// vent på N-1 replies (fra alle peerIDs)
	for {
		time.Sleep(15 * time.Millisecond)
		n.mu.Lock()
		allOK := true
		for _, pid := range peerIDs {
			if !n.replies[pid] {
				allOK = false
				break
			}
		}
		if allOK {
			n.state = Held
			n.mu.Unlock()
			log.Printf("[GOT_ALL] node=%d ts=%d", myID, myTs)
			return true
		}
		n.mu.Unlock()

		select {
		case <-ctx.Done():
			log.Printf("[TIMEOUT] EnterCS node=%d ts=%d err=%v", myID, myTs, ctx.Err())
			return false
		default:
		}
	}
}

// RA: ud af CS (flush deferred) – lazy dial ved flush
func (n *Node) ExitCS(ctx context.Context) {
	n.mu.Lock()
	n.state = Released
	toFlush := append([]int(nil), n.deferred...)
	n.deferred = nil

	// bump clock for udsendelser
	n.clock++
	ts := n.clock

	// snapshot port map fra konfigurerede peers
	ports := make(map[int]string)
	for _, p := range n.peers {
		if id, err := strconv.Atoi(p); err == nil {
			ports[id] = p
		}
	}
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, pid := range toFlush {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			port := ports[pid]
			if port == "" { // ukendt peer-id ift. konfiguration
				log.Printf("[FLUSH_SKIP] unknown peer id=%d", pid)
				return
			}
			cli := n.getOrDialClient(port)
			if cli == nil {
				log.Printf("[FLUSH_SKIP] reply -> %d (no client)", pid)
				return
			}
			_, _ = cli.Reply(context.Background(), &pb.ReplyMsg{
				SenderId:  int32(n.id),
				Timestamp: ts,
				Content:   "ok",
			})
			log.Printf("[FLUSH] reply -> %d (deferred)", pid)
		}(pid)
	}
	wg.Wait()
}

//
// ---------- Server (RPC handlers) ----------
//

type Server struct {
	pb.UnimplementedRicartagrawalaServer
	node *Node
}

func startServer(n *Node) {
	// Bind eksplicit på IPv4 loopback
	lis, err := net.Listen("tcp4", "127.0.0.1:"+n.port)
	if err != nil {
		log.Fatalf("Failed to listen on 127.0.0.1:%s: %v", n.port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRicartagrawalaServer(grpcServer, &Server{node: n})

	log.Printf("Node listening on 127.0.0.1:%s", n.port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

func (s *Server) Request(ctx context.Context, req *pb.RequestMsg) (*pb.Ack, error) {
	n := s.node

	before := n.getClock()
	n.onRecv(req.Timestamp)
	after := n.getClock()

	n.mu.Lock()
	fromID := int(req.SenderId)

	// Defer-regel: Held, eller Wanted og (min (ts,id) < deres (ts,id))
	deferIt := (n.state == Held) ||
		(n.state == Wanted && (n.myReqTs < req.Timestamp ||
			(n.myReqTs == req.Timestamp && n.id < fromID)))

	if deferIt {
		n.deferred = append(n.deferred, fromID)
		state := n.state
		myTs := n.myReqTs
		n.mu.Unlock()
		log.Printf("[RECV_REQ] from=%d -> defer=true me={id=%d state=%v myTs=%d} lamport:%d->%d",
			fromID, n.id, state, myTs, before, after)
		return &pb.Ack{Ok: true}, nil
	}

	// Ellers svar straks (Reply) — MEN: vi må ikke kalde getOrDialClient mens lås holdes
	peerPort := strconv.Itoa(fromID)
	state := n.state
	n.clock++
	sendTs := n.clock
	n.mu.Unlock()

	log.Printf("[RECV_REQ] from=%d -> defer=false SEND_REPLY me={id=%d state=%v} lamport:%d->%d(send=%d)",
		fromID, n.id, state, before, after, sendTs)

	cli := n.getOrDialClient(peerPort)

	if cli != nil {
		go func() {
			_, err := cli.Reply(context.Background(), &pb.ReplyMsg{
				SenderId:  int32(n.id),
				Timestamp: sendTs,
				Content:   "ok",
			})
			if err != nil {
				log.Printf("[SEND_FAIL] reply -> %d err=%v", fromID, err)
			} else {
				log.Printf("[SEND_OK] reply -> %d", fromID)
			}
		}()
	} else {
		log.Printf("[NO_CLIENT] reply stub missing for peer %d", fromID)
	}
	return &pb.Ack{Ok: true}, nil
}

func (s *Server) Reply(ctx context.Context, rep *pb.ReplyMsg) (*pb.Ack, error) {
	n := s.node

	before := n.getClock()
	n.onRecv(rep.Timestamp)
	after := n.getClock()

	n.mu.Lock()
	if n.replies == nil {
		n.replies = make(map[int]bool)
	}
	n.replies[int(rep.SenderId)] = true
	n.mu.Unlock()

	log.Printf("[RECV_REP] from=%d lamport:%d->%d", rep.SenderId, before, after)
	return &pb.Ack{Ok: true}, nil
}

//
// ---------- main (bootstrap + REPL) ----------
//

func main() {
	in := bufio.NewReader(os.Stdin)

	// 1) egen port
	fmt.Print("Type port address to listen at: ")
	myPort, _ := in.ReadString('\n')
	myPort = strings.TrimSpace(myPort)

	// 2) peers
	fmt.Print("Type other ports to connect to: ")
	rawPeers, _ := in.ReadString('\n')
	rawPeers = strings.TrimSpace(rawPeers)
	var peers []string
	if rawPeers != "" {
		peers = strings.Fields(rawPeers)
	}

	myID, _ := strconv.Atoi(myPort)
	node := &Node{id: myID, port: myPort, peers: peers}

	// 3) server
	go startServer(node)

	// 4) klient-forbindelser (best-effort; lazy dial dækker resten)
	//node.connectToPeers()

	// 5) REPL
	sc := bufio.NewScanner(os.Stdin)
	fmt.Println("Commands: req | exit")
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			break
		}
		cmd := strings.TrimSpace(sc.Text())

		switch cmd {
		case "req":
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			ok := node.EnterCS(ctx)
			cancel()
			if !ok {
				continue
			}
			log.Printf("[CS] %d ENTER", node.id)
			time.Sleep(2 * time.Second) // simuler CS-arbejde
			log.Printf("[CS] %d EXIT", node.id)
			node.ExitCS(context.Background())

		case "exit":
			return

		default:
			fmt.Println("Commands: req | exit")
		}
	}
}

//
// ---------- lazy dial helper ----------
//

func (n *Node) getOrDialClient(peerPort string) pb.RicartagrawalaClient {
	n.mu.Lock()
	cli := n.clients[peerPort]
	n.mu.Unlock()
	if cli != nil {
		return cli
	}

	addr := "127.0.0.1:" + peerPort
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // vent indtil forbindelsen er klar
	)
	cancel()
	if err != nil {
		log.Printf("[DIAL_ON_DEMAND_FAIL] %s err=%v", addr, err)
		return nil
	}
	cli = pb.NewRicartagrawalaClient(conn)
	n.mu.Lock()
	if n.clients == nil {
		n.clients = make(map[string]pb.RicartagrawalaClient)
	}
	n.clients[peerPort] = cli
	n.mu.Unlock()
	log.Printf("[DIAL_ON_DEMAND_OK] %s", addr)
	return cli
}
