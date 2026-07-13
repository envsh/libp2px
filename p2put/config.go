package p2put

import (
	"flag"
	"os"
	"time"

	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

type Config struct {
	// usage1, just Fset.parse()
	fset *flag.FlagSet // caller parser

	// usage2, assign direct, without Fset.parse
	KeyFile    string // fedkey seed file
	ListenPort int
	HubName    string // VlanName, our every nodes same name for find
	IsMobile   bool   // bandwidth and battary
	UserAgent  string // "universal-connectivity/go-peer"

	Dht      bool
	ResRate  float32 // 0.5, 1, 2
	Relay    bool
	NAT      bool
	Udp      bool
	Tcp      bool
	Wss      bool
	Quic     bool
	Bw       bool
	Punching bool
	AutoPing bool

	Topics []string

	enableTurnRelay bool
}

// default for vps deploy that not cmdline
var dftConfig = Config{
	KeyFile:    "key.txt",
	HubName:    "envsh-d2hub",
	UserAgent:  "universal-connectivity/envsh-d2hub",
	ListenPort: defaultListenPort,
	Dht:        true,
	ResRate:   0.2,
	Relay:     true,
	NAT:       true,
	Tcp:       true,
	Punching:  true,
}

const officalHubName = "universal-connectivity"

var currConfig = dftConfig

func getFlagSet(cfg *Config) *flag.FlagSet {
	fs := flag.NewFlagSet("libp2p-node", flag.ContinueOnError)
	fs.StringVar(&cfg.KeyFile, "k", "key.txt", "keyring file")
	fs.IntVar(&cfg.ListenPort, "l", defaultListenPort, "TCP listen port (default 4001)")
	fs.BoolVar(&cfg.IsMobile, "m", cfg.IsMobile, "Run mobile mode, less bandwidth")

	return fs
}

func ConfigFlags() (*Config, *flag.FlagSet) {
    fs := getFlagSet(&currConfig)
	currConfig.fset = fs
    return &currConfig, fs
}

// deprecated
func ParseConfig() Config {
	currConfig.fset = getFlagSet(&currConfig)
	err := currConfig.fset.Parse(os.Args[1:])
	if err != nil {
		os.Exit(0)
	}
	return currConfig
}

// ///
// usage: libp2p.New(libp2p.ResourceManager(myResourcemanager()),
func myResourceManager() network.ResourceManager {
	limits := rcmgr.DefaultLimits
	syslmt := limits.SystemBaseLimit
	var rate = 4
	if currConfig.IsMobile {
		rate = 1 // or 2
	}
	limits.SystemBaseLimit = rcmgr.BaseLimit{
		Conns:           (syslmt.Conns / 4) * rate,
		ConnsInbound:    (syslmt.ConnsInbound / 4) * rate,
		ConnsOutbound:   (syslmt.ConnsOutbound / 4) * rate,
		Streams:         (syslmt.Streams / 4) * rate,
		StreamsInbound:  (syslmt.StreamsInbound / 4) * rate,
		StreamsOutbound: (syslmt.StreamsOutbound / 4) * rate,
		FD:              (syslmt.FD / 4) * rate,
		Memory:          (syslmt.Memory / 4) * int64(rate),
	}
	rm, _ := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.Scale(0, 0)))

	return rm
}

func myGossipSubParams() pubsub.GossipSubParams {
	dft := pubsub.DefaultGossipSubParams()
	dft0 := dft
	which := 0 // 0 default 1 minimal 2 middle
	if currConfig.IsMobile {
		which = 2 // or 1
	}
	if which == 1 { // lower bw
		dft.D = 3
		dft.Dlo = 2
		dft.Dhi = 4
		dft.Dlazy = 2
		dft.GossipFactor = 0.05                 // 0.02
		dft.HeartbeatInterval = 5 * time.Second // 10
		dft.HistoryLength = 3                   // 2
		dft.HistoryGossip = 2                   // 1
		dft.DirectConnectTicks = 600            // ← 必须，否则除以零
	} else if which == 2 {
		dft.D = 3
		dft.Dlo = 2
		dft.Dhi = 6
		dft.Dlazy = 3
		dft.GossipFactor = 0.1
		dft.HeartbeatInterval = 2 * time.Second
		dft.HistoryLength = 5
		dft.HistoryGossip = 3
		dft.DirectConnectTicks = 600 // ← 必须，否则除以零
	} else { // default
		dft.D = 3
		dft.Dlo = 2
		dft.Dhi = 6
		dft.Dlazy = 6
		dft.GossipFactor = 0.25
		dft.HeartbeatInterval = 10 * time.Second
		dft.HistoryLength = 5
		dft.HistoryGossip = 3
		dft.DirectConnectTicks = 600 // ← 必须，否则除以零

		dft = dft0
	}
	return dft
}
