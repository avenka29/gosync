package gosync

import (
	"strconv"
	"sync"
	"time"

	"github.com/avenka29/gosync/internal/socketio"
)

// RecoveryConfig controls bounded recovery state held by one server process.
type RecoveryConfig struct {
	MaxDisconnectionDuration time.Duration
	MaxSessions              int
	MaxPacketsPerSession     int
	MaxBytes                 int64
}

type replayPacket struct {
	offset  string
	encoded socketio.EncodedPacket
	size    int64
}
type recoveryState struct {
	id           string
	pid          string
	namespace    string
	rooms        []string
	disconnected time.Time
	packets      []replayPacket
}
type recoveryStore struct {
	mu       sync.Mutex
	config   RecoveryConfig
	sessions map[string]*recoveryState
	sequence uint64
	bytes    int64
}

func newRecovery(config *RecoveryConfig) (*recoveryStore, error) {
	if config == nil {
		return nil, nil
	}
	c := *config
	if c.MaxDisconnectionDuration < 0 || c.MaxSessions < 0 || c.MaxPacketsPerSession < 0 || c.MaxBytes < 0 {
		return nil, ErrInvalidConfig
	}
	if c.MaxDisconnectionDuration == 0 {
		c.MaxDisconnectionDuration = 2 * time.Minute
	}
	if c.MaxSessions == 0 {
		c.MaxSessions = 1000
	}
	if c.MaxPacketsPerSession == 0 {
		c.MaxPacketsPerSession = 256
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 8 << 20
	}
	return &recoveryStore{config: c, sessions: make(map[string]*recoveryState)}, nil
}

func (r *recoveryStore) remove(pid string) {
	if state := r.sessions[pid]; state != nil {
		for _, packet := range state.packets {
			r.bytes -= packet.size
		}
		delete(r.sessions, pid)
	}
}

func (r *recoveryStore) expire() {
	now := time.Now()
	for pid, state := range r.sessions {
		if !state.disconnected.IsZero() && now.Sub(state.disconnected) > r.config.MaxDisconnectionDuration {
			r.remove(pid)
		}
	}
}

func (r *recoveryStore) attach(s *Socket) []replayPacket {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expire()
	auth, _ := s.auth.(map[string]any)
	pid, _ := auth["pid"].(string)
	offset, _ := auth["offset"].(string)
	if state := r.sessions[pid]; state != nil && state.namespace == s.namespace.name && !state.disconnected.IsZero() {
		for index, packet := range state.packets {
			if packet.offset == offset {
				s.id = state.id
				s.pid = state.pid
				s.recovered = true
				for _, room := range state.rooms {
					s.rooms[room] = struct{}{}
				}
				state.disconnected = time.Time{}
				return append([]replayPacket(nil), state.packets[index+1:]...)
			}
		}
	}
	if len(r.sessions) >= r.config.MaxSessions {
		return nil
	}
	pid, err := newID()
	if err != nil {
		return nil
	}
	s.pid = pid
	r.sessions[pid] = &recoveryState{id: s.id, pid: pid, namespace: s.namespace.name}
	return nil
}

func (r *recoveryStore) detach(s *Socket, rooms []string, recoverable bool) {
	if r == nil || s.pid == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !recoverable {
		r.remove(s.pid)
		return
	}
	if state := r.sessions[s.pid]; state != nil {
		state.rooms = rooms
		state.disconnected = time.Now()
	}
}

func (r *recoveryStore) record(pid string, packet socketio.Packet, codec socketio.Codec) (socketio.Packet, error) {
	if r == nil || pid == "" || packet.Type != socketio.PacketEvent || packet.ID != nil {
		return packet, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expire()
	state := r.sessions[pid]
	if state == nil {
		return packet, nil
	}
	r.sequence++
	offset := strconv.FormatUint(r.sequence, 36)
	packet.Data = append(append([]any(nil), packet.Data.([]any)...), offset)
	encoded, err := codec.Encode(packet)
	if err != nil {
		return packet, err
	}
	size := int64(len(encoded.Header))
	for _, b := range encoded.Attachments {
		size += int64(len(b))
	}
	for len(state.packets) > 0 && (len(state.packets) >= r.config.MaxPacketsPerSession || r.bytes+size > r.config.MaxBytes) {
		old := state.packets[0]
		state.packets = state.packets[1:]
		r.bytes -= old.size
	}
	if r.bytes+size > r.config.MaxBytes {
		r.remove(pid)
		return packet, nil
	}
	state.packets = append(state.packets, replayPacket{offset: offset, encoded: encoded, size: size})
	r.bytes += size
	return packet, nil
}

func (r *recoveryStore) broadcast(b *Broadcast, event string, args []any) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.expire()
	var pids []string
	for pid, state := range r.sessions {
		if state.disconnected.IsZero() || state.namespace != b.namespace.name || state.id == b.sender {
			continue
		}
		include := len(b.rooms) == 0
		for _, room := range state.rooms {
			for _, want := range b.rooms {
				if room == want {
					include = true
				}
			}
		}
		for _, room := range state.rooms {
			for _, exclude := range b.except {
				if room == exclude {
					include = false
					goto selected
				}
			}
		}
	selected:
		if include {
			pids = append(pids, pid)
		}
	}
	r.mu.Unlock()
	for _, pid := range pids {
		_, err := r.record(pid, newEventPacket(b.namespace.name, event, args, nil), b.namespace.server.codec)
		b.namespace.server.report(err)
	}
}
