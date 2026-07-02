modular libp2p instance creation, so many options

### usage

maybe need minute to find peers.

curl 'localhost:4004/p2pin/events?topic=foo,bar,baz'
curl 'localhost:4004/p2pin/send?topic=foo' -d 'hello world'

### bandwidth too high than tox

go-libp2p 0.36.2 based:

maybe x5+

go-libp2p 0.46 upstream: need test

不再适用，瘦节点-m模式，带宽比tox低3-5倍，但完整节点依旧带宽高

### not bootstrap but active node

* /ip4/65.109.60.254/tcp/4001 12D3KooWL96RJHMjvPzkDzEwSBNei4Ftak7n8gF5Tfn8Dc1cSYQn

### big plan

* remove webrtc/quic code from libp2p.git
* update to libp2p v0.49, about 2026.06

### note: don't use go 1.25.6 build
