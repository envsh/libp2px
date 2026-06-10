package relay

import (
	"errors"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var validity = 30 * time.Minute

var (
	errTooManyReservations        = errors.New("too many reservations")
	errTooManyReservationsForPeer = errors.New("too many reservations for peer")
	errTooManyReservationsForIP   = errors.New("too many peers for IP address")

)

// constraints implements various reservation constraints
type constraints struct {
	rc *Resources

	mutex sync.Mutex
	total []time.Time
	peers map[peer.ID][]time.Time
	ips   map[string][]time.Time
}

// newConstraints creates a new constraints object.
// The methods are *not* thread-safe; an external lock must be held if synchronization
// is required.
func newConstraints(rc *Resources) *constraints {
	return &constraints{
		rc:    rc,
		peers: make(map[peer.ID][]time.Time),
		ips:   make(map[string][]time.Time),
	}
}

// AddReservation adds a reservation for a given peer with a given multiaddr.
// If adding this reservation violates IP constraints, an error is returned.
func (c *constraints) AddReservation(p peer.ID, a ma.Multiaddr) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	c.cleanup(now)

	if len(c.total) >= c.rc.MaxReservations {
		return errTooManyReservations
	}

	ip, err := manet.ToIP(a)
	if err != nil {
		return errors.New("no IP address associated with peer")
	}

	peerReservations := c.peers[p]
	if len(peerReservations) >= c.rc.MaxReservationsPerPeer {
		return errTooManyReservationsForPeer
	}

	ipReservations := c.ips[ip.String()]
	if len(ipReservations) >= c.rc.MaxReservationsPerIP {
		return errTooManyReservationsForIP
	}
	expiry := now.Add(validity)
	c.total = append(c.total, expiry)

	peerReservations = append(peerReservations, expiry)
	c.peers[p] = peerReservations

	ipReservations = append(ipReservations, expiry)
	c.ips[ip.String()] = ipReservations
	return nil
}

func (c *constraints) cleanupList(l []time.Time, now time.Time) []time.Time {
	var index int
	for i, t := range l {
		if t.After(now) {
			break
		}
		index = i + 1
	}
	return l[index:]
}

func (c *constraints) cleanup(now time.Time) {
	c.total = c.cleanupList(c.total, now)
	for k, peerReservations := range c.peers {
		c.peers[k] = c.cleanupList(peerReservations, now)
	}
	for k, ipReservations := range c.ips {
		c.ips[k] = c.cleanupList(ipReservations, now)
	}
}
