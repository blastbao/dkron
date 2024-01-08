package dkron

import (
	"fmt"
	"net"
	"sync"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const (
	// barrierWriteTimeout is used to give Raft a chance to process a
	// possible loss of leadership event if we are unable to get a barrier
	// while leader.
	barrierWriteTimeout = 2 * time.Minute
)

// monitorLeadership is used to monitor if we acquire or lose our role
// as the leader in the Raft cluster. There is some work the leader is
// expected to do, so we must react to changes
// 处理 leader 变化
func (a *Agent) monitorLeadership() {
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		a.logger.Info("dkron: monitoring leadership")
		select {
		case isLeader := <-a.leaderCh:
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					a.logger.Error("dkron: attempted to start the leader loop while running")
					continue
				}
				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					a.leaderLoop(ch)
				}(weAreLeaderCh)
				a.logger.Info("dkron: cluster leadership acquired")
			default:
				if weAreLeaderCh == nil {
					a.logger.Error("dkron: attempted to stop the leader loop while not running")
					continue
				}
				a.logger.Debug("dkron: shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				a.logger.Info("dkron: cluster leadership lost")
			}
		case <-a.shutdownCh:
			return
		}
	}
}

// leaderLoop runs as long as we are the leader to run various
// maintenance activities
// 处理变成 leader 事件
func (a *Agent) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member
	establishedLeader := false

RECONCILE:

	// Setup a reconciliation timer
	reconcileCh = nil
	interval := time.After(a.config.ReconcileInterval)

	// Apply a raft barrier to ensure our FSM is caught up
	// [重要] 等待 raft 集群中大部分达成一致，也就是当下集群中只有一个 leader ，也即当前节点。
	start := time.Now()
	barrier := a.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		a.logger.WithError(err).Error("dkron: failed to wait for barrier")
		goto WAIT
	}
	metrics.MeasureSince([]string{"dkron", "leader", "barrier"}, start)

	// Check if we need to handle initial leadership actions
	// 启动 leader 的工作任务，把 jobs 添加到 cron 中并启动
	if !establishedLeader {
		if err := a.establishLeadership(stopCh); err != nil {
			a.logger.WithError(err).Error("dkron: failed to establish leadership")
			// Immediately revoke leadership since we didn't successfully establish leadership.
			if err := a.revokeLeadership(); err != nil {
				a.logger.WithError(err).Error("dkron: failed to revoke leadership")
			}
			goto WAIT
		}
		establishedLeader = true // 已经启动
		defer func() {
			if err := a.revokeLeadership(); err != nil {
				a.logger.WithError(err).Error("dkron: failed to revoke leadership")
			}
		}()
	}

	// Reconcile any missing data
	// 同步全局状态，确保 serf 和 raft 的 cluster 集群状态一致。
	if err := a.reconcile(); err != nil {
		a.logger.WithError(err).Error("dkron: failed to reconcile")
		goto WAIT
	}

	// Initial reconcile worked, now we can process the channel updates
	reconcileCh = a.reconcileCh

	// Poll the stop channel to give it priority so we don't waste time
	// trying to perform the other operations if we have been asked to shut down.
	select {
	case <-stopCh:
		return
	default:
	}

WAIT:
	// Wait until leadership is lost
	for {
		select {
		case <-stopCh:
			return
		case <-a.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			a.reconcileMember(member) // 根据 serf 节点状态更新 raft
		}
	}
}

// reconcile is used to reconcile the differences between Serf
// membership and what is reflected in our strongly consistent store.
func (a *Agent) reconcile() error {
	defer metrics.MeasureSince([]string{"dkron", "leader", "reconcile"}, time.Now())
	members := a.serf.Members()
	for _, member := range members {
		if err := a.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

// reconcileMember is used to do an async reconcile of a single serf member
func (a *Agent) reconcileMember(member serf.Member) error {
	// 1. 忽略本机

	// 2. 根据 m.Tags 做一些过滤：IP/Port 是否合法、DC/Region 是否匹配、Version 是否匹配 ...
	// Check if this is a member we should handle
	valid, parts := isServer(member)
	if !valid || parts.Region != a.config.Region {
		return nil
	}
	defer metrics.MeasureSince([]string{"dkron", "leader", "reconcileMember"}, time.Now())

	// 3. 根据节点状态更新 raft
	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = a.addRaftPeer(member, parts)
	case serf.StatusLeft:
		err = a.removeRaftPeer(member, parts)
	}
	if err != nil {
		a.logger.WithError(err).WithField("member", member).Error("failed to reconcile member")
		return err
	}

	return nil
}

// establishLeadership is invoked once we become leader and are able
// to invoke an initial barrier. The barrier is used to ensure any
// previously inflight transactions have been committed and that our
// state is up-to-date.
//
// 一旦成为领导者并能够发起初始的 barrier 操作，就会调用 establishLeadership 方法。
// barrier 操作用于确保之前正在进行中的事务已经提交，并且当前状态是最新的。
func (a *Agent) establishLeadership(stopCh chan struct{}) error {
	defer metrics.MeasureSince([]string{"dkron", "leader", "establish_leadership"}, time.Now())
	a.logger.Info("agent: Starting scheduler")

	// 查询所有 Jobs
	jobs, err := a.Store.GetJobs(nil)
	if err != nil {
		a.logger.Fatal(err)
	}
	// 将 jobs 添加到 cron 后并启动
	a.sched.Start(jobs, a)
	return nil
}

// revokeLeadership is invoked once we step down as leader.
// This is used to cleanup any state that may be specific to a leader.
func (a *Agent) revokeLeadership() error {
	defer metrics.MeasureSince([]string{"dkron", "leader", "revoke_leadership"}, time.Now())
	// 停止 cron
	a.sched.Stop()
	return nil
}

// addRaftPeer is used to add a new Raft peer when a dkron server joins
// 当一个新的 dkron 服务器（即一个 Serf 节点）加入时，会执行 addRaftPeer 函数，用于将该节点添加为 Raft 集群中的一个 Peer 节点。
//
//	首先，在添加节点之前会检查当前集群中是否已经存在一个处于 Bootstrap 模式的节点。
//	如果存在，则不允许再添加新的节点。这是由于当多个节点同时处于 Bootstrap 模式时，可能会导致集群状态不一致或者发生脑裂等问题。
//	所以，boostrap 节点只能有一个。
//
//	然后，将该节点的地址和端口转换为 TCP 地址，并获取当前 Raft 集群的配置信息。
//	如果当前节点正在加入 Raft 集群，则检查当前集群是否至少有三个节点，如果不足，则返回不处理。
//	这是因为在 Raft 中最少需要三个节点才能进行共识算法。
//
//	接着，检查当前节点是否已经存在于 Raft 集群中。如果已经存在，无需重复添加。
//	如果地址相同但 ID 不同，则需要先将旧节点从 Raft 集群中删除，然后再添加新节点。
//
//	最后，将该节点添加为新的 Raft Peer 节点。
//	如果添加成功，则该节点会参与 Raft 算法的投票和日志复制。
//	如果添加失败，则会返回相应的错误信息。
func (a *Agent) addRaftPeer(m serf.Member, parts *ServerParts) error {
	// Check for possibility of multiple bootstrap nodes
	// 检查是否有可能有多个引导节点。引导节点是负责初始化集群的第一个节点，如果有多个引导节点，则不添加新 peer ，避免一致性问题。
	var members = a.serf.Members()
	if parts.Bootstrap {
		for _, member := range members {
			valid, p := isServer(member)
			if valid && member.Name != m.Name && p.Bootstrap {
				a.logger.Errorf("dkron: '%v' and '%v' are both in bootstrap mode. Only one node should be in bootstrap mode, not adding Raft peer.", m.Name, member.Name)
				return nil
			}
		}
	}

	// Processing ourselves could result in trying to remove ourselves to
	// fix up our address, which would make us step down. This is only
	// safe to attempt if there are multiple servers available.
	addr := (&net.TCPAddr{IP: m.Addr, Port: parts.Port}).String()
	configFuture := a.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		a.logger.WithError(err).Error("dkron: failed to get raft configuration")
		return err
	}

	if m.Name == a.config.NodeName {
		if l := len(configFuture.Configuration().Servers); l < 3 {
			a.logger.WithField("peer", m.Name).Debug("dkron: Skipping self join check since the cluster is too small")
			return nil
		}
	}

	// See if it's already in the configuration. It's harmless to re-add it
	// but we want to avoid doing that if possible to prevent useless Raft
	// log entries. If the address is the same but the ID changed, remove the
	// old server before adding the new one.
	// 遍历当前 Raft 配置，检查新节点是否已经在集群中。如果已经在集群中，那么不需要再次添加。
	for _, server := range configFuture.Configuration().Servers {
		// If the address or ID matches an existing server, see if we need to remove the old one first
		if server.Address == raft.ServerAddress(addr) || server.ID == raft.ServerID(parts.ID) {
			// Exit with no-op if this is being called on an existing server and both the ID and address match
			// 若 IP 和 ID 均相同，意味着当前节点已经存在于集群中，无需添加到 raft 集群中。
			if server.Address == raft.ServerAddress(addr) && server.ID == raft.ServerID(parts.ID) {
				return nil
			}
			// 否则，意味着发生冲突，需要删除当前在 raft 集群中的节点，再重新添加。
			// 因为在 Raft 集群中，每个节点必须具有唯一的 ID ，如果新增节点与现有节点具有相同的 ID ，则需要先删除现有节点，然后再添加新节点。
			future := a.raft.RemoveServer(server.ID, 0, 0)
			if server.Address == raft.ServerAddress(addr) {
				if err := future.Error(); err != nil {
					return fmt.Errorf("error removing server with duplicate address %q: %s", server.Address, err)
				}
				a.logger.WithField("server", server.Address).Info("dkron: removed server with duplicate address")
			} else {
				if err := future.Error(); err != nil {
					return fmt.Errorf("error removing server with duplicate ID %q: %s", server.ID, err)
				}
				a.logger.WithField("server", server.ID).Info("dkron: removed server with duplicate ID")
			}
		}
	}

	// Attempt to add as a peer
	// 尝试将新节点添加到 Raft 集群中。
	// 如果添加成功，则新节点将成为 Raft集 群的一部分，能够参与 Leader 选举和日志复制。
	switch {
	case minRaftProtocol >= 3:
		addFuture := a.raft.AddVoter(raft.ServerID(parts.ID), raft.ServerAddress(addr), 0, 0)
		if err := addFuture.Error(); err != nil {
			a.logger.WithError(err).Error("dkron: failed to add raft peer")
			return err
		}
	}

	return nil
}

// removeRaftPeer is used to remove a Raft peer when a dkron server leaves
// or is reaped
func (a *Agent) removeRaftPeer(m serf.Member, parts *ServerParts) error {
	// See if it's already in the configuration. It's harmless to re-remove it
	// but we want to avoid doing that if possible to prevent useless Raft
	// log entries.
	configFuture := a.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		a.logger.WithError(err).Error("dkron: failed to get raft configuration")
		return err
	}

	// Pick which remove API to use based on how the server was added.
	for _, server := range configFuture.Configuration().Servers {
		// If we understand the new add/remove APIs and the server was added by ID, use the new remove API
		if minRaftProtocol >= 2 && server.ID == raft.ServerID(parts.ID) {
			a.logger.WithField("server", server.ID).Info("dkron: removing server by ID")
			future := a.raft.RemoveServer(raft.ServerID(parts.ID), 0, 0)
			if err := future.Error(); err != nil {
				a.logger.WithError(err).WithField("server", server.ID).Error("dkron: failed to remove raft peer")
				return err
			}
			break
		}
	}

	return nil
}
