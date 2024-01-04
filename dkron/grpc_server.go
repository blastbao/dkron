package dkron

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/distribworks/dkron/v3/plugin"
	proto "github.com/distribworks/dkron/v3/plugin/types"
	pb "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	// ErrExecutionDoneForDeletedJob is returned when an execution done
	// is received for a non existent job.
	ErrExecutionDoneForDeletedJob = errors.New("grpc: Received execution done for a deleted job")
	// ErrRPCDialing is returned on dialing fail.
	ErrRPCDialing = errors.New("grpc: Error dialing, verify the network connection to the server")
	// ErrNotLeader is the error returned when the operation need the node to be the leader,
	// but the current node is not the leader.
	ErrNotLeader = errors.New("grpc: Error, server is not leader, this operation should be run on the leader")
	// ErrBrokenStream is the error that indicates a sudden disconnection of the agent streaming an execution
	ErrBrokenStream = errors.New("grpc: Error on execution streaming, agent connection was abruptly terminated")
)

// DkronGRPCServer defines the basics that a gRPC server should implement.
type DkronGRPCServer interface {
	proto.DkronServer
	Serve(net.Listener) error
}

// GRPCServer is the local implementation of the gRPC server interface.
type GRPCServer struct {
	proto.DkronServer
	agent  *Agent
	logger *logrus.Entry
}

// NewGRPCServer creates and returns an instance of a DkronGRPCServer implementation
func NewGRPCServer(agent *Agent, logger *logrus.Entry) DkronGRPCServer {
	return &GRPCServer{
		agent:  agent,
		logger: logger,
	}
}

// Serve creates and start a new gRPC dkron server
func (grpcs *GRPCServer) Serve(lis net.Listener) error {
	grpcServer := grpc.NewServer()
	// 注册 dkron 接口
	proto.RegisterDkronServer(grpcServer, grpcs)

	as := NewAgentServer(grpcs.agent, grpcs.logger)
	// 注册 agent 接口
	proto.RegisterAgentServer(grpcServer, as)
	go grpcServer.Serve(lis)

	return nil
}

// Encode is used to encode a Protoc object with type prefix
// Proto 转换成 Bytes
func Encode(t MessageType, msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	m, err := pb.Marshal(msg.(pb.Message))
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(m)
	return buf.Bytes(), err
}

// SetJob broadcast a state change to the cluster members that will store the job.
// Then restart the scheduler
// This only works on the leader
func (grpcs *GRPCServer) SetJob(ctx context.Context, setJobReq *proto.SetJobRequest) (*proto.SetJobResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "set_job"}, time.Now())
	grpcs.logger.WithFields(logrus.Fields{
		"job": setJobReq.Job.Name,
	}).Debug("grpc: Received SetJob")

	// 1. 提交到 raft cluster ，具体 raft 执行逻辑，详见 `SetJobType` 相关流程（主要是存储到 store 中）。
	if err := grpcs.agent.applySetJob(setJobReq.Job); err != nil {
		return nil, err
	}

	// 2. 注册 cron 任务
	// If everything is ok, add the job to the scheduler
	job := NewJobFromProto(setJobReq.Job)
	job.Agent = grpcs.agent
	if err := grpcs.agent.sched.AddJob(job); err != nil {
		return nil, err
	}

	// 3. 返回成功
	return &proto.SetJobResponse{}, nil
}

// DeleteJob broadcast a state change to the cluster members that will delete the job.
// This only works on the leader
func (grpcs *GRPCServer) DeleteJob(ctx context.Context, delJobReq *proto.DeleteJobRequest) (*proto.DeleteJobResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "delete_job"}, time.Now())
	grpcs.logger.WithField("job", delJobReq.GetJobName()).Debug("grpc: Received DeleteJob")

	// 1. 构造 raft cmd
	cmd, err := Encode(DeleteJobType, delJobReq)
	if err != nil {
		return nil, err
	}

	// 2. 提交到 raft cluster ，只有 leader 才能调用 apply()
	af := grpcs.agent.raft.Apply(cmd, raftTimeout)
	if err := af.Error(); err != nil {
		return nil, err
	}

	// 3. 解析响应
	res := af.Response()
	job, ok := res.(*Job)
	if !ok {
		return nil, fmt.Errorf("grpc: Error wrong response from apply in DeleteJob: %v", res)
	}
	jpb := job.ToProto()

	// 4. 从 cron 中移除
	// [待确认] cron 是每次选举出 leader 时重新加载的吗？
	// [待确认] 向 cron 中增加、移除任务是实时同步到 store 的吗？怎么保证集群中一致？
	// [待确认] 初步看在 leader.go 中能找到答案。
	// If everything is ok, remove the job
	grpcs.agent.sched.RemoveJob(job)
	return &proto.DeleteJobResponse{Job: jpb}, nil
}

// GetJob loads the job from the datastore
func (grpcs *GRPCServer) GetJob(ctx context.Context, getJobReq *proto.GetJobRequest) (*proto.GetJobResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "get_job"}, time.Now())
	grpcs.logger.WithField("job", getJobReq.JobName).Debug("grpc: Received GetJob")

	// 1. 直接从本地 store 中查询 job ，raft 保证集群中每个节点的存储都是强一致的
	j, err := grpcs.agent.Store.GetJob(getJobReq.JobName, nil)
	if err != nil {
		return nil, err
	}

	// 2. 构造响应
	gjr := &proto.GetJobResponse{
		Job: &proto.Job{},
	}

	// Copy the data structure
	gjr.Job.Name = j.Name
	gjr.Job.Executor = j.Executor
	gjr.Job.ExecutorConfig = j.ExecutorConfig

	return gjr, nil
}

// ExecutionDone saves the execution to the store
// 执行完成, 进行后续处理
func (grpcs *GRPCServer) ExecutionDone(ctx context.Context, execDoneReq *proto.ExecutionDoneRequest) (*proto.ExecutionDoneResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "execution_done"}, time.Now())
	grpcs.logger.WithFields(logrus.Fields{
		"group": execDoneReq.Execution.Group,
		"job":   execDoneReq.Execution.JobName,
		"from":  execDoneReq.Execution.NodeName,
	}).Debug("grpc: Received execution done")

	// Get the leader address and compare with the current node address.
	// Forward the request to the leader in case current node is not the leader.
	// 如果当前节点不是领导者，则将请求转发给领导者。
	if !grpcs.agent.IsLeader() {
		addr := grpcs.agent.raft.Leader()
		grpcs.agent.GRPCClient.ExecutionDone(string(addr), NewExecutionFromProto(execDoneReq.Execution))
		return nil, ErrNotLeader
	}
	// 当前节点是 leader 。

	// This is the leader at this point, so process the execution, encode the value and apply the log to the cluster.
	// Get the defined output types for the job, and call them
	// 1. 获取 job 信息
	job, err := grpcs.agent.Store.GetJob(execDoneReq.Execution.JobName, nil)
	if err != nil {
		return nil, err
	}

	// 2. 遍历所有的 job 处理器，对请求进行处理，处理器是通过插件实现的。
	// 如果处理器存在，则调用处理器对请求进行处理，否则记录错误日志。
	pbex := *execDoneReq.Execution
	for k, v := range job.Processors {
		grpcs.logger.WithField("plugin", k).Info("grpc: Processing execution with plugin")
		if processor, ok := grpcs.agent.ProcessorPlugins[k]; ok {
			v["reporting_node"] = grpcs.agent.config.NodeName
			pbex = processor.Process(&plugin.ProcessorArgs{Execution: pbex, Config: v})
		} else {
			grpcs.logger.WithField("plugin", k).Error("grpc: Specified plugin not found")
		}
	}

	// 3. 将执行结果提交到 raft cluster
	execDoneReq.Execution = &pbex
	cmd, err := Encode(ExecutionDoneType, execDoneReq)
	if err != nil {
		return nil, err
	}
	af := grpcs.agent.raft.Apply(cmd, raftTimeout)
	if err := af.Error(); err != nil {
		return nil, err
	}

	// 4. 获取最新 job 信息
	// Retrieve the fresh, updated job from the store to work on stored values
	job, err = grpcs.agent.Store.GetJob(job.Name, nil)
	if err != nil {
		grpcs.logger.WithError(err).WithField("job", execDoneReq.Execution.JobName).Error("grpc: Error retrieving job from store")
		return nil, err
	}

	// 5. 检查执行是否失败，如果执行失败且重试次数小于 Job 最大重试次数，则重试。
	// If the execution failed, retry it until retries limit (default: don't retry)
	execution := NewExecutionFromProto(&pbex)
	if !execution.Success && execution.Attempt < job.Retries+1 {
		// 重试次数 +1
		execution.Attempt++
		execution.Output = "" // Keep all execution properties intact except the last output
		grpcs.logger.WithFields(logrus.Fields{
			"attempt":   execution.Attempt,
			"execution": execution,
		}).Debug("grpc: Retrying execution")
		// 执行 execution
		if _, err := grpcs.agent.Run(job.Name, execution); err != nil {
			return nil, err
		}
		// 构造响应
		return &proto.ExecutionDoneResponse{
			From:    grpcs.agent.config.NodeName,
			Payload: []byte("retry"),
		}, nil
	}

	// 提取出和 execution 属于同一个 group 的其它 executions
	exg, err := grpcs.agent.Store.GetExecutionGroup(execution, &ExecutionOptions{Timezone: job.GetTimeLocation()})
	if err != nil {
		grpcs.logger.WithError(err).WithField("group", execution.Group).Error("grpc: Error getting execution group.")
		return nil, err
	}

	// 6. 发送执行完毕的通知(email/webhook)
	// Send notification
	if err := Notification(grpcs.agent.config, execution, exg, job).Send(grpcs.logger); err != nil {
		return nil, err
	}

	// 7. 执行依赖的任务
	// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin execution dependent jobs on success.
	if len(job.DependentJobs) > 0 && job.Status == StatusSuccess {
		for _, djn := range job.DependentJobs {
			dj, err := grpcs.agent.Store.GetJob(djn, nil)
			dj.Agent = grpcs.agent
			if err != nil {
				return nil, err
			}
			grpcs.logger.WithField("job", djn).Debug("grpc: Running dependent job")
			dj.Run()
		}
	}

	return &proto.ExecutionDoneResponse{
		From:    grpcs.agent.config.NodeName,
		Payload: []byte("saved"),
	}, nil
}

// Leave calls the Stop method, stopping everything in the server
func (grpcs *GRPCServer) Leave(ctx context.Context, in *empty.Empty) (*empty.Empty, error) {
	return in, grpcs.agent.Stop()
}

// RunJob runs a job in the cluster
func (grpcs *GRPCServer) RunJob(ctx context.Context, req *proto.RunJobRequest) (*proto.RunJobResponse, error) {
	ex := NewExecution(req.JobName)
	job, err := grpcs.agent.Run(req.JobName, ex)
	if err != nil {
		return nil, err
	}
	jpb := job.ToProto()
	return &proto.RunJobResponse{Job: jpb}, nil
}

// ToggleJob toggle the enablement of a job
func (grpcs *GRPCServer) ToggleJob(ctx context.Context, getJobReq *proto.ToggleJobRequest) (*proto.ToggleJobResponse, error) {
	return nil, nil
}

// RaftGetConfiguration get raft config
func (grpcs *GRPCServer) RaftGetConfiguration(ctx context.Context, in *empty.Empty) (*proto.RaftGetConfigurationResponse, error) {
	// We can't fetch the leader and the configuration atomically with
	// the current Raft API.
	future := grpcs.agent.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	// Index the information about the servers.
	serverMap := make(map[raft.ServerAddress]serf.Member)
	for _, member := range grpcs.agent.serf.Members() {
		valid, parts := isServer(member)
		if !valid {
			continue
		}

		addr := (&net.TCPAddr{IP: member.Addr, Port: parts.Port}).String()
		serverMap[raft.ServerAddress(addr)] = member
	}

	// Fill out the reply.
	leader := grpcs.agent.raft.Leader()
	reply := &proto.RaftGetConfigurationResponse{}
	reply.Index = future.Index()
	for _, server := range future.Configuration().Servers {
		node := "(unknown)"
		raftProtocolVersion := "unknown"
		if member, ok := serverMap[server.Address]; ok {
			node = member.Name
			if raftVsn, ok := member.Tags["raft_vsn"]; ok {
				raftProtocolVersion = raftVsn
			}
		}

		entry := &proto.RaftServer{
			Id:           string(server.ID),
			Node:         node,
			Address:      string(server.Address),
			Leader:       server.Address == leader,
			Voter:        server.Suffrage == raft.Voter,
			RaftProtocol: raftProtocolVersion,
		}
		reply.Servers = append(reply.Servers, entry)
	}
	return reply, nil
}

// RaftRemovePeerByID is used to kick a stale peer (one that is in the Raft
// quorum but no longer known to Serf or the catalog) by address in the form of
// "IP:port". The reply argument is not used, but is required to fulfill the RPC
// interface.
func (grpcs *GRPCServer) RaftRemovePeerByID(ctx context.Context, in *proto.RaftRemovePeerByIDRequest) (*empty.Empty, error) {
	// Since this is an operation designed for humans to use, we will return
	// an error if the supplied id isn't among the peers since it's
	// likely they screwed up.
	{
		future := grpcs.agent.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			return nil, err
		}
		for _, s := range future.Configuration().Servers {
			if s.ID == raft.ServerID(in.Id) {
				goto REMOVE
			}
		}
		return nil, fmt.Errorf("id %q was not found in the Raft configuration", in.Id)
	}

REMOVE:
	// The Raft library itself will prevent various forms of foot-shooting,
	// like making a configuration with no voters. Some consideration was
	// given here to adding more checks, but it was decided to make this as
	// low-level and direct as possible. We've got ACL coverage to lock this
	// down, and if you are an operator, it's assumed you know what you are
	// doing if you are calling this. If you remove a peer that's known to
	// Serf, for example, it will come back when the leader does a reconcile
	// pass.
	future := grpcs.agent.raft.RemoveServer(raft.ServerID(in.Id), 0, 0)
	if err := future.Error(); err != nil {
		grpcs.logger.WithError(err).WithField("peer", in.Id).Warn("failed to remove Raft peer")
		return nil, err
	}

	grpcs.logger.WithField("peer", in.Id).Warn("removed Raft peer")
	return new(empty.Empty), nil
}

// GetActiveExecutions returns the active executions on the server node
// 返回本 server 活跃的执行清单
func (grpcs *GRPCServer) GetActiveExecutions(ctx context.Context, in *empty.Empty) (*proto.GetActiveExecutionsResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "agent_run"}, time.Now())

	var executions []*proto.Execution
	grpcs.agent.activeExecutions.Range(func(k, v interface{}) bool {
		e := v.(*proto.Execution)
		executions = append(executions, e)
		return true
	})

	return &proto.GetActiveExecutionsResponse{
		Executions: executions,
	}, nil
}

// SetExecution broadcast a state change to the cluster members that will store the execution.
// This only works on the leader
// 储存任务执行状态
func (grpcs *GRPCServer) SetExecution(ctx context.Context, execution *proto.Execution) (*empty.Empty, error) {
	defer metrics.MeasureSince([]string{"grpc", "set_execution"}, time.Now())
	grpcs.logger.WithFields(logrus.Fields{
		"execution": execution.Key(),
	}).Debug("grpc: Received SetExecution")

	// 构造 Raft Msg := MsgType(1B) + MsgData
	cmd, err := Encode(SetExecutionType, execution)
	if err != nil {
		grpcs.logger.WithError(err).Fatal("agent: encode error in SetExecution")
		return nil, err
	}

	// 通过 raft apply 同步集群状态
	af := grpcs.agent.raft.Apply(cmd, raftTimeout)
	if err := af.Error(); err != nil {
		grpcs.logger.WithError(err).Fatal("agent: error applying SetExecutionType")
		return nil, err
	}

	return new(empty.Empty), nil
}
