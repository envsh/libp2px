# AGENTS.md — libp2px

## Progress

### Done
- `RelayPool` with SLRU two-tier architecture (probation/main), EMA scoring, circuit breaker, 6-dimension weighting, epsilon-greedy+roulette selection, 3-round main protection pruning
- `SelectN()` returns `[]peer.AddrInfo` sorted by score descending, skipping circuitOpen items
- `relayPoolManager` active with K=3, `watchStaticRelays` disabled (commented out)

## Key Decisions

- **relayPoolManager**: 60s ticker, only manages `ListManaged()` (up to K=3), `SelectN()` to fill vacancies, circuitOpen auto-disconnect, no oscillation (never kicks healthy relays)
- **Managed set**: `AddManaged`/`RemoveManaged`/`ListManaged` tracks the K relays that relayPoolManager actively manages; separate from pool items
- **SelectN**: returns `[]peer.AddrInfo` (not `[]multiaddr.Multiaddr`), sorted by score descending, skips circuitOpen

## Next Steps

- Remove `watchStaticRelays` entirely (currently commented out) once relayPoolManager is validated in production
- Hook `myEventSuber` disconnection events into `RecordResult` for faster fault detection

## Relevant Files

- `p2put/relaypool.go`: full RelayPool implementation (SLRU, scoring, pruning, select, circuit breaker, health check)
- `p2put/libp2p_bs.go:960-1050`: relayPoolManager, doRelayReserve, pidsFromAddrs
- `p2put/libp2p_bs.go:888-954`: watchStaticRelays (currently commented out at line 336)
- `docs/relaypool-design.md`: detailed design doc for RelayPool
