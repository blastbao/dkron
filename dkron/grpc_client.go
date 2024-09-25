package dkron

import (
	"fmt"
	"io"
	"time"

	metrics "github.com/armon/go-metrics"
	proto "github.com/distribworks/dkron/v3/plugin/types"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// DkronGRPCClient defines the interface that any gRPC client for dkron should implement.
//
// 集群中任一节点都持有 `GRPCClient` ，用来向 Raft Leader 发送请求。
type DkronGRPCClient interface {
	Connect(string) (*grpc.ClientConn, error)
	ExecutionDone(string, *Execution) error
	GetJob(string, string) (*Job, error)
	SetJob(*Job) error
	DeleteJob(string) (*Job, error)
	Leave(string) error
	RunJob(string) (*Job, error)
	RaftGetConfiguration(string) (*proto.RaftGetConfigurationResponse, error)
	RaftRemovePeerByID(string, string) error
	GetActiveExecutions(string) ([]*proto.Execution, error)
	SetExecution(execution *proto.Execution) error
	AgentRun(addr string, job *proto.Job, execution *proto.Execution) error
}

// GRPCClient is the local implementation of the DkronGRPCClient interface.
type GRPCClient struct {
	dialOpt []grpc.DialOption
	agent   *Agent
	logger  *logrus.Entry
}

// NewGRPCClient returns a new instance of the gRPC client.
func NewGRPCClient(dialOpt grpc.DialOption, agent *Agent, logger *logrus.Entry) DkronGRPCClient {
	if dialOpt == nil {
		dialOpt = grpc.WithInsecure()
	}
	return &GRPCClient{
		dialOpt: []grpc.DialOption{
			dialOpt,
			grpc.WithBlock(),
		},
		agent:  agent,
		logger: logger,
	}
}

// Connect dialing to a gRPC server
func (grpcc *GRPCClient) Connect(addr string) (*grpc.ClientConn, error) {
	// Initiate a connection with the server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpcc.dialOpt...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// ExecutionDone calls the ExecutionDone gRPC method
func (grpcc *GRPCClient) ExecutionDone(addr string, execution *Execution) error {
	defer metrics.MeasureSince([]string{"grpc", "call_execution_done"}, time.Now())
	var conn *grpc.ClientConn

	// 建立 grpc conn
	conn, err := grpcc.Connect(addr)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "ExecutionDone",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}

	// 封装 grpc client ，调用 rpc `ExecutionDone`
	d := proto.NewDkronClient(conn)
	edr, err := d.ExecutionDone(context.Background(), &proto.ExecutionDoneRequest{Execution: execution.ToProto()})
	if err != nil {
		if err.Error() == fmt.Sprintf("rpc error: code = Unknown desc = %s", ErrNotLeader.Error()) {
			grpcc.logger.Info("grpc: ExecutionDone forwarded to the leader")
			conn.Close()
			return nil
		}
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "ExecutionDone",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return err
	}
	grpcc.logger.WithFields(logrus.Fields{
		"method":      "ExecutionDone",
		"server_addr": addr,
		"from":        edr.From,
		"payload":     string(edr.Payload),
	}).Debug("grpc: Response from method")

	// 关闭连接
	conn.Close()
	return nil
}

// GetJob calls GetJob gRPC method in the server
func (grpcc *GRPCClient) GetJob(addr, jobName string) (*Job, error) {
	defer metrics.MeasureSince([]string{"grpc", "get_job"}, time.Now())
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	conn, err := grpcc.Connect(addr)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "GetJob",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return nil, err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	gjr, err := d.GetJob(context.Background(), &proto.GetJobRequest{JobName: jobName})
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "GetJob",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return nil, err
	}

	return NewJobFromProto(gjr.Job), nil
}

// Leave calls Leave method on the gRPC server
func (grpcc *GRPCClient) Leave(addr string) error {
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	conn, err := grpcc.Connect(addr)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "Leave",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	_, err = d.Leave(context.Background(), &empty.Empty{})
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "Leave",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return err
	}

	return nil
}

// SetJob calls the leader passing the job
func (grpcc *GRPCClient) SetJob(job *Job) error {
	var conn *grpc.ClientConn

	// 让 leader 操作 job
	addr := grpcc.agent.raft.Leader()

	// Initiate a connection with the server
	conn, err := grpcc.Connect(string(addr))
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "SetJob",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	_, err = d.SetJob(context.Background(), &proto.SetJobRequest{
		Job: job.ToProto(),
	})
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "SetJob",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return err
	}
	return nil
}

// DeleteJob calls the leader passing the job name
func (grpcc *GRPCClient) DeleteJob(jobName string) (*Job, error) {
	var conn *grpc.ClientConn

	// 让 leader 操作 job
	addr := grpcc.agent.raft.Leader()

	// Initiate a connection with the server
	conn, err := grpcc.Connect(string(addr))
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "DeleteJob",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return nil, err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	res, err := d.DeleteJob(context.Background(), &proto.DeleteJobRequest{
		JobName: jobName,
	})
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "DeleteJob",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return nil, err
	}

	job := NewJobFromProto(res.Job)

	return job, nil
}

// RunJob calls the leader passing the job name
//
// [重要]
func (grpcc *GRPCClient) RunJob(jobName string) (*Job, error) {
	var conn *grpc.ClientConn

	// 取 leader 的 addr
	addr := grpcc.agent.raft.Leader()

	// 建立同 leader 的 grpc conn
	conn, err := grpcc.Connect(string(addr))
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "RunJob",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return nil, err
	}
	defer conn.Close()

	// 封装 grpc client ，并调用 `RunJob` rpc ，同步等待执行结果
	d := proto.NewDkronClient(conn)
	res, err := d.RunJob(context.Background(), &proto.RunJobRequest{
		JobName: jobName,
	})
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "RunJob",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return nil, err
	}

	// 返回最新 job 信息
	job := NewJobFromProto(res.Job)
	return job, nil
}

// RaftGetConfiguration get the current raft configuration of peers
func (grpcc *GRPCClient) RaftGetConfiguration(addr string) (*proto.RaftGetConfigurationResponse, error) {
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	conn, err := grpcc.Connect(addr)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "RaftGetConfiguration",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return nil, err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	res, err := d.RaftGetConfiguration(context.Background(), &empty.Empty{})
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "RaftGetConfiguration",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return nil, err
	}

	return res, nil
}

// RaftRemovePeerByID remove a raft peer
func (grpcc *GRPCClient) RaftRemovePeerByID(addr, peerID string) error {
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	conn, err := grpcc.Connect(addr)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "RaftRemovePeerByID",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	_, err = d.RaftRemovePeerByID(context.Background(),
		&proto.RaftRemovePeerByIDRequest{Id: peerID},
	)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "RaftRemovePeerByID",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return err
	}

	return nil
}

// GetActiveExecutions returns the active executions of a server node
func (grpcc *GRPCClient) GetActiveExecutions(addr string) ([]*proto.Execution, error) {
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	conn, err := grpcc.Connect(addr)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "GetActiveExecutions",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return nil, err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	gaer, err := d.GetActiveExecutions(context.Background(), &empty.Empty{})
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "GetActiveExecutions",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return nil, err
	}

	return gaer.Executions, nil
}

// SetExecution calls the leader passing the execution
func (grpcc *GRPCClient) SetExecution(execution *proto.Execution) error {
	var conn *grpc.ClientConn

	// 获取 raft leader 的 addr
	addr := grpcc.agent.raft.Leader()

	// Initiate a connection with the server
	// 建立 grpc conn
	conn, err := grpcc.Connect(string(addr))
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "SetExecution",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}
	defer conn.Close()

	// Synchronous call
	// 封装 grpc client ，调用 `SetExecution` rpc
	d := proto.NewDkronClient(conn)
	_, err = d.SetExecution(context.Background(), execution)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "SetExecution",
			"server_addr": addr,
		}).Error("grpc: Error calling gRPC method")
		return err
	}
	return nil
}

// AgentRun runs a job in the given agent
// Dkron server 调用此方法通过 GRPC 下发 Job 到 server/agent 执行
func (grpcc *GRPCClient) AgentRun(addr string, job *proto.Job, execution *proto.Execution) error {
	defer metrics.MeasureSince([]string{"grpc_client", "agent_run"}, time.Now())
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	// (MAYO): remove string type wrap
	//
	// 1. 建立 grpc connection
	conn, err := grpcc.Connect(addr)
	if err != nil {
		grpcc.logger.WithError(err).WithFields(logrus.Fields{
			"method":      "AgentRun",
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}
	defer conn.Close()

	// Streaming call
	//
	// 2. 封装 grpc client ，调用 rpc `AgentRun` 提交任务到 addr 节点上执行
	a := proto.NewAgentClient(conn)
	stream, err := a.AgentRun(context.Background(), &proto.AgentRunRequest{
		Job:       job,
		Execution: execution,
	})
	if err != nil {
		return err
	}

	// 3. 实时接收执行状态
	var first bool
	for {
		ars, err := stream.Recv()

		// Stream ends
		// 3.1 执行成功，发送 done 命令给 leader ，记录执行成功信息，返回
		if err == io.EOF {
			addr := grpcc.agent.raft.Leader()
			if err := grpcc.ExecutionDone(string(addr), NewExecutionFromProto(execution)); err != nil {
				return err
			}
			return nil
		}

		// Error received from the stream
		// 3.2 执行失败，发送 done 命令给 leader ，记录执行失败信息，返回
		if err != nil {
			// At this point the execution status will be unknown, set the FinshedAt time and an explanatory message
			execution.FinishedAt = ptypes.TimestampNow()
			execution.Output = []byte(err.Error())

			grpcc.logger.WithError(err).Error(ErrBrokenStream)

			addr := grpcc.agent.raft.Leader()
			if err := grpcc.ExecutionDone(string(addr), NewExecutionFromProto(execution)); err != nil {
				return err
			}
			return err
		}

		// 3.3 执行中，记录 ars

		// Registers an active stream
		// 更新 execution 最新状态
		grpcc.agent.activeExecutions.Store(ars.Execution.Key(), ars.Execution)
		grpcc.logger.WithField("key", ars.Execution.Key()).Debug("grpc: received execution stream")

		execution = ars.Execution
		defer grpcc.agent.activeExecutions.Delete(execution.Key())

		// Store the received execution in the raft log and store
		if !first {
			// 4. 从 stream 中收到的首个消息，是 exec 执行前的初始状态，这里转发给 leader 让其记录下这个状态。
			if err := grpcc.SetExecution(ars.Execution); err != nil {
				return err
			}
			first = true
		}
	}
}
