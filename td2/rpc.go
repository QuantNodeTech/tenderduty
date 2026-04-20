package tenderduty

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"time"

	dash "github.com/firstset/tenderduty/v2/td2/dashboard"
	rpchttp "github.com/tendermint/tendermint/rpc/client/http"
)

// maxNodeStaleness is the maximum age of a node's latest block before it is
// considered stale. A node that reports catching_up=false but hasn't produced
// a block recently is treated the same as a lagging node.
const maxNodeStaleness = 5 * time.Minute

// newRpc sets up the rpc client used for monitoring. It probes all configured
// nodes and picks the one with the highest (freshest) block height, preferring
// nodes whose latest block is within maxNodeStaleness.
func (cc *ChainConfig) newRpc() error {
	if cc.cosmosDirectoryData == nil {
		if err := cc.loadCosmosDirectoryData(); err != nil {
			l(slog.LevelWarn, "ℹ️ cosmos.directory data not available for", cc.name, "(chain_name: "+cc.getEffectiveChainName()+", chain_id: "+cc.ChainId+")", "-", err)
		} else {
			l(slog.LevelInfo, "✅ loaded cosmos.directory data for", cc.name, "(chain_name: "+cc.getEffectiveChainName()+", chain_id: "+cc.ChainId+")")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var anyWorking bool
	for _, endpoint := range cc.Nodes {
		anyWorking = anyWorking || !endpoint.down
	}

	markDown := func(endpoint *NodeConfig, msg string) {
		if !endpoint.down {
			endpoint.down = true
			endpoint.downSince = time.Now()
		}
		endpoint.lastMsg = msg
	}

	type candidate struct {
		client    *rpchttp.HTTP
		height    int64
		blockTime time.Time
		endpoint  *NodeConfig
		url       string
	}

	// probeUrl returns a candidate if the node is reachable, on the right chain,
	// and not catching up. Returns nil + logs on any failure.
	probeUrl := func(u string, endpoint *NodeConfig) *candidate {
		if _, err := url.Parse(u); err != nil {
			msg := fmt.Sprintf("❌ could not parse url %s: (%s) %s", cc.name, u, err)
			l(slog.LevelInfo, msg)
			if endpoint != nil {
				markDown(endpoint, msg)
			}
			return nil
		}
		client, err := rpchttp.New(u, "/websocket")
		if err != nil {
			msg := fmt.Sprintf("❌ could not connect client for %s: (%s) %s", cc.name, u, err)
			l(slog.LevelInfo, msg)
			if endpoint != nil {
				markDown(endpoint, msg)
			}
			return nil
		}

		var ns nodeStatus
		status, err := client.Status(ctx)
		if err != nil {
			ns, err = getStatusWithEndpoint(ctx, u)
			if err != nil {
				msg := fmt.Sprintf("❌ could not get status for %s: (%s) %s", cc.name, u, err)
				l(slog.LevelInfo, msg)
				if endpoint != nil {
					markDown(endpoint, msg)
				}
				return nil
			}
		} else {
			ns = nodeStatus{
				network:    status.NodeInfo.Network,
				catchingUp: status.SyncInfo.CatchingUp,
				height:     status.SyncInfo.LatestBlockHeight,
				blockTime:  status.SyncInfo.LatestBlockTime,
			}
		}

		if ns.network != cc.ChainId {
			msg := fmt.Sprintf("chain id %s on %s does not match, expected %s, skipping", ns.network, u, cc.ChainId)
			l(slog.LevelInfo, msg)
			if endpoint != nil {
				markDown(endpoint, msg)
			}
			return nil
		}
		if ns.catchingUp {
			msg := fmt.Sprint("🐢 node is not synced, skipping ", u)
			l(slog.LevelInfo, msg)
			if endpoint != nil {
				endpoint.syncing = true
				markDown(endpoint, msg)
			}
			return nil
		}
		return &candidate{client: client, height: ns.height, blockTime: ns.blockTime, endpoint: endpoint, url: u}
	}

	var candidates []*candidate
	for _, endpoint := range cc.Nodes {
		if anyWorking && endpoint.down {
			continue
		}
		if c := probeUrl(endpoint.Url, endpoint); c != nil {
			candidates = append(candidates, c)
		}
	}

	// Pick best candidate: prefer non-stale nodes, then highest block height.
	pickBest := func(pool []*candidate) *candidate {
		var best *candidate
		for _, c := range pool {
			if best == nil || c.height > best.height {
				best = c
			}
		}
		return best
	}

	now := time.Now()
	var fresh []*candidate
	for _, c := range candidates {
		if !c.blockTime.IsZero() && now.Sub(c.blockTime) <= maxNodeStaleness {
			fresh = append(fresh, c)
		}
	}

	best := pickBest(fresh)
	if best == nil {
		best = pickBest(candidates)
		if best != nil {
			lag := now.Sub(best.blockTime).Round(time.Second)
			l(slog.LevelWarn, fmt.Sprintf("⚠️ all nodes for %s are stale, using best available %s (lag %v)", cc.name, best.url, lag))
		}
	}

	if best != nil {
		cc.client = best.client
		cc.noNodes = false
		if len(candidates) > 1 {
			l(slog.LevelInfo, fmt.Sprintf("✅ selected freshest node for %s: %s (height %d)", cc.name, best.url, best.height))
		}
		return nil
	}

	// No configured node worked — try cosmos.directory fallback
	{
		chainName := cc.getEffectiveChainName()
		u := getRegistryUrlByChainName(chainName)
		node := guessPublicEndpoint(u)
		l(slog.LevelInfo, cc.ChainId, "⛑ attempting to use cosmos.directory fallback node (chain_name:", chainName+")", node)
		if c := probeUrl(node, nil); c != nil {
			cc.client = c.client
			cc.noNodes = false
			l(slog.LevelInfo, cc.ChainId, "⛑ connected to cosmos.directory endpoint", node)
			return nil
		}
		l(slog.LevelWarn, "⚠️ could not connect to cosmos.directory fallback for chain_name:", chainName)
	}

	// Legacy fallback using chain_id lookup (when PublicFallback is explicitly enabled)
	if cc.PublicFallback {
		if u, ok := getRegistryUrl(cc.ChainId); ok {
			node := guessPublicEndpoint(u)
			l(slog.LevelInfo, cc.ChainId, "⛑ attempting to use public fallback node (chain_id lookup)", node)
			if c := probeUrl(node, nil); c != nil {
				cc.client = c.client
				cc.noNodes = false
				l(slog.LevelInfo, cc.ChainId, "⛑ connected to public endpoint", node)
				return nil
			}
		} else {
			l(slog.LevelWarn, "could not find a public endpoint for", cc.ChainId)
		}
	}
	cc.noNodes = true
	alarms.clearAll(cc.name)
	cc.lastError = "no usable RPC endpoints available for " + cc.ChainId
	if td.EnableDash {
		td.updateChan <- &dash.ChainStatus{
			MsgType:                 "status",
			Name:                    cc.name,
			ChainId:                 cc.ChainId,
			Moniker:                 cc.valInfo.Moniker,
			Bonded:                  cc.valInfo.Bonded,
			Jailed:                  cc.valInfo.Jailed,
			Tombstoned:              cc.valInfo.Tombstoned,
			Missed:                  cc.valInfo.Missed,
			Window:                  cc.valInfo.Window,
			MinSignedPerWindow:      cc.minSignedPerWindow,
			Nodes:                   len(cc.Nodes),
			HealthyNodes:            0,
			ActiveAlerts:            1,
			Height:                  0,
			LastError:               cc.lastError,
			Blocks:                  cc.blocksResults,
			UnvotedOpenGovProposals: len(cc.unvotedOpenGovProposals),
			TotalBondedTokens:       cc.totalBondedTokens,
			TotalSupply:             cc.totalSupply,
			CommunityTax:            cc.communityTax,
			InflationRate:           cc.inflationRate,
			BaseAPR:                 cc.baseAPR,
			VotingPowerPercent:      cc.valInfo.VotingPowerPercent,
			DelegatedTokens:         cc.valInfo.DelegatedTokens,
			CommissionRate:          cc.valInfo.CommissionRate,
			ValidatorAPR:            cc.valInfo.ValidatorAPR,
			SelfDelegationRewards:   cc.valInfo.SelfDelegationRewards,
			Commission:              cc.valInfo.Commission,
			CryptoPrice:             cc.cryptoPrice,
			DenomMetadata:           cc.denomMetadata,
			Projected30DRewards:     cc.valInfo.Projected30DRewards,
		}
	}
	return errors.New("no usable endpoints available for " + cc.ChainId)
}

func (cc *ChainConfig) monitorHealth(ctx context.Context, chainName string) {
	tick := time.NewTicker(time.Minute)
	if cc.client == nil {
		_ = cc.newRpc()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			var err error
			for _, node := range cc.Nodes {
				go func(node *NodeConfig) {
					alert := func(msg string) {
						node.lastMsg = fmt.Sprintf("%-12s node %s is %s", chainName, node.Url, msg)
						if !node.AlertIfDown {
							// even if we aren't alerting, we want to display the status in the dashboard.
							node.down = true
							return
						}
						if !node.down {
							node.down = true
							node.downSince = time.Now()
						}
						if td.Prom {
							td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, time.Since(node.downSince).Seconds(), node.Url)
						}
						l(slog.LevelWarn, "⚠️ "+node.lastMsg)
					}
					c, e := rpchttp.New(node.Url, "/websocket")
					if e != nil {
						alert(e.Error())
					}
					cwt, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					status, e := c.Status(cwt)
					cancel()
					if e != nil {
						alert("down")
						return
					}
					if status.NodeInfo.Network != cc.ChainId {
						alert("on the wrong network")
						return
					}
					if status.SyncInfo.CatchingUp {
						alert("not synced")
						node.syncing = true
						return
					}
					if lag := time.Since(status.SyncInfo.LatestBlockTime); lag > maxNodeStaleness {
						alert(fmt.Sprintf("stale (last block %v ago)", lag.Round(time.Second)))
						node.syncing = true
						return
					}

					// node's OK, clear the note
					if node.down {
						node.lastMsg = ""
						node.wasDown = true
					}
					td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, 0, node.Url)
					node.down = false
					node.syncing = false
					node.downSince = time.Unix(0, 0)
					cc.noNodes = false
					l(slog.LevelInfo, fmt.Sprintf("🟢 %-12s node %s is healthy", chainName, node.Url))
				}(node)
			}

			if cc.client == nil {
				e := cc.newRpc()
				if e != nil {
					l(slog.LevelError, "💥", cc.ChainId, e)
				}
			} else {
				// Re-select if the active client has become stale
				sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
				if st, err := cc.client.Status(sctx); err == nil {
					if time.Since(st.SyncInfo.LatestBlockTime) > maxNodeStaleness {
						l(slog.LevelWarn, fmt.Sprintf("⚠️ active node for %s is stale (lag %v), reconnecting", cc.name, time.Since(st.SyncInfo.LatestBlockTime).Round(time.Second)))
						cc.client = nil
						if e := cc.newRpc(); e != nil {
							l(slog.LevelError, "💥", cc.ChainId, e)
						}
					}
				}
				scancel()
			}
			if cc.valInfo != nil {
				cc.lastValInfo = &ValInfo{
					Moniker:               cc.valInfo.Moniker,
					Bonded:                cc.valInfo.Bonded,
					Jailed:                cc.valInfo.Jailed,
					Tombstoned:            cc.valInfo.Tombstoned,
					Missed:                cc.valInfo.Missed,
					Window:                cc.valInfo.Window,
					Conspub:               cc.valInfo.Conspub,
					Valcons:               cc.valInfo.Valcons,
					DelegatedTokens:       cc.valInfo.DelegatedTokens,
					VotingPowerPercent:    cc.valInfo.VotingPowerPercent,
					CommissionRate:        cc.valInfo.CommissionRate,
					SelfDelegationRewards: cc.valInfo.SelfDelegationRewards,
					Commission:            cc.valInfo.Commission,
				}
			}
			err = cc.GetValInfo(false)
			if err != nil {
				l(slog.LevelWarn, "❓ refreshing signing info for", cc.ValAddress, err)
			}
		}
	}
}

func (c *Config) pingHealthcheck() {
	if !c.Healthcheck.Enabled {
		return
	}

	ticker := time.NewTicker(c.Healthcheck.PingRate * time.Second)

	go func() {
		for range ticker.C {
			_, err := http.Get(c.Healthcheck.PingURL)
			if err != nil {
				l(slog.LevelWarn, fmt.Sprintf("❌ Failed to ping healthcheck URL: %s", err.Error()))
			} else {
				l(slog.LevelInfo, fmt.Sprintf("🏓 Successfully pinged healthcheck URL: %s", c.Healthcheck.PingURL))
			}
		}
	}()
}

// endpointRex matches the first a tag's hostname and port if present.
var endpointRex = regexp.MustCompile(`//([^/:]+)(:\d+)?`)

// guessPublicEndpoint attempts to deal with a shortcoming in the tendermint RPC client that doesn't allow path prefixes.
// The cosmos.directory requires them. This is a workaround to get the actual URL for the server behind their proxy.
// The RPC base URL will return links endpoints, and we can parse this to guess the original URL.
func guessPublicEndpoint(u string) string {
	resp, err := http.Get(u + "/")
	if err != nil {
		return u
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return u
	}
	_ = resp.Body.Close()
	matches := endpointRex.FindStringSubmatch(string(b))
	if len(matches) < 2 {
		// didn't work
		return u
	}
	proto := "https://"
	port := ":443"
	// will be 3 elements if there is a port no port means listening on https
	if len(matches) == 3 && matches[2] != "" && matches[2] != ":443" {
		proto = "http://"
		port = matches[2]
	}
	return proto + matches[1] + port
}

type nodeStatus struct {
	network    string
	catchingUp bool
	height     int64
	blockTime  time.Time
}

func getStatusWithEndpoint(ctx context.Context, u string) (nodeStatus, error) {
	parsedURL, err := url.Parse(u)
	if err != nil {
		return nodeStatus{}, err
	}
	if parsedURL.Scheme == "tcp" {
		parsedURL.Scheme = "http"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/status", parsedURL.String()), nil)
	if err != nil {
		return nodeStatus{}, err
	}
	tr := &http.Transport{
		//#nosec G402 -- configurable option
		TLSClientConfig: &tls.Config{InsecureSkipVerify: td.TLSSkipVerify},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Do(req) //#nosec G704 -- URL is from operator-supplied config
	if err != nil {
		return nodeStatus{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nodeStatus{}, err
	}
	type tendermintStatus struct {
		Result struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"node_info"`
			SyncInfo struct {
				LatestBlockHeight string    `json:"latest_block_height"`
				LatestBlockTime   time.Time `json:"latest_block_time"`
				CatchingUp        bool      `json:"catching_up"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	var s tendermintStatus
	if err := json.Unmarshal(b, &s); err != nil {
		return nodeStatus{}, err
	}
	var height int64
	fmt.Sscanf(s.Result.SyncInfo.LatestBlockHeight, "%d", &height)
	return nodeStatus{
		network:    s.Result.NodeInfo.Network,
		catchingUp: s.Result.SyncInfo.CatchingUp,
		height:     height,
		blockTime:  s.Result.SyncInfo.LatestBlockTime,
	}, nil
}
