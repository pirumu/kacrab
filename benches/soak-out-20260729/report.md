# kacrab soak run

- duration: 18002s (rate target 1000/s, value 512B, 6 partitions, RF3 min.insync=2)
- chaos: broker kill every 600s (45s downtime) on ["kacrab-kafka2", "kacrab-kafka3"]; consumer bounce every 900s

## Verdict: FAIL

| metric | value |
|---|---|
| produced | 18000000 |
| acked | 17976328 |
| send rejects | 0 |
| delivery errors | 23672 |
| consumed | 18019503 |
| duplicates (at-least-once re-reads) | 49057 |
| reordered (gaps later refilled) | 133689 |
| **losses (gaps never refilled)** | **29554** |
| unconsumed tail at end | 0 |
| parse errors | 0 |
| producer retries (lib) | 0 |
| producer errors (lib) | 0 |
| rebalances observed | 8 |
| consumer restarts | 45 |
| consumer-group wedges | 13 |
| chaos events | 163 |

Time series in `soak.csv`; chaos timeline in `events.log`.

Unfilled gap ranges: p0: 856609..=856616; p0: 1098214..=1098244; p0: 1384242..=1384249; p0: 1947359..=1947366; p0: 1947717..=1947724; p0: 2045400..=2045408; p0: 2147350..=2151295; p0: 2153776..=2153806; p0: 2159914..=2159944; p0: 2188093..=2188123; p0: 2189581..=2189611; p0: 2204151..=2204181; p0: 2271080..=2271110; p0: 2271886..=2271916; p0: 2295694..=2295724; p0: 2306575..=2306605; p0: 2312155..=2312185; p0: 2313519..=2313549; p0: 2319967..=2319997; p0: 2341884..=2341914; p0: 2343434..=2343464; p0: 2585884..=2585891; p1: 1098214..=1098244; p1: 2045409..=2045416; p1: 2147350..=2151295; p1: 2153776..=2153806; p1: 2159914..=2159944; p1: 2170144..=2170205; p1: 2183753..=2183783; p1: 2188527..=2188557; p1: 2189581..=2189611; p1: 2204151..=2204181; p1: 2215714..=2215744; p1: 2218287..=2218317; p1: 2236298..=2236328; p1: 2271080..=2271110; p1: 2271886..=2271916; p1: 2274645..=2274675; p1: 2295694..=2295724; p1: 2306575..=2306605; p1: 2313519..=2313549; p1: 2319998..=2320028; p1: 2332615..=2332645; p1: 2341884..=2341914; p1: 2343434..=2343464; p2: 1098213..=1098243; p2: 1613392..=1613416; p2: 1947667..=1947674; p2: 2147350..=2151294; p2: 2153775..=2153805; … (truncated)
