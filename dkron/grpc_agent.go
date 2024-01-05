package dkron

import (
	"errors"
	"time"

	"github.com/armon/circbuf"
	metrics "github.com/armon/go-metrics"
	"github.com/distribworks/dkron/v3/plugin/types"
	"github.com/golang/protobuf/ptypes"
	"github.com/sirupsen/logrus"
)

const (
	// maxBufSize limits how much data we collect from a handler.
	maxBufSize = 256000
)

type statusAgentHelper struct {
	execution *types.Execution
	stream    types.Agent_AgentRunServer
}

func (s *statusAgentHelper) Update(b []byte, c bool) (int64, error) {
	s.execution.Output = b
	// Send partial execution
	if err := s.stream.Send(&types.AgentRunStream{
		Execution: s.execution,
	}); err != nil {
		return 0, err
	}
	return 0, nil
}

// AgentServer is the local implementation of the gRPC server interface.
type AgentServer struct {
	types.AgentServer
	agent  *Agent
	logger *logrus.Entry
}

// NewAgentServer creates and returns an instance of a DkronGRPCServer implementation
func NewAgentServer(agent *Agent, logger *logrus.Entry) types.AgentServer {
	return &AgentServer{
		agent:  agent,
		logger: logger,
	}
}

// AgentRun is called when an agent starts running a job and lasts all execution,
// the agent will stream execution progress to the server.
//
// AgentRun 在 agent 开始运行作业时被调用，并持续整个执行过程，agent 将实时将执行进度流式传输到服务器。
func (as *AgentServer) AgentRun(req *types.AgentRunRequest, stream types.Agent_AgentRunServer) error {
	defer metrics.MeasureSince([]string{"grpc_agent", "agent_run"}, time.Now())

	job := req.Job
	execution := req.Execution

	as.logger.WithFields(logrus.Fields{
		"job": job.Name,
	}).Info("grpc_agent: Starting job")

	output, _ := circbuf.NewBuffer(maxBufSize)
	var success bool
	jex := job.Executor
	exc := job.ExecutorConfig

	// Send the first update with the initial execution state to be stored in the server
	execution.StartedAt = ptypes.TimestampNow()
	execution.NodeName = as.agent.config.NodeName

	// 发送执行前状态
	if err := stream.Send(&types.AgentRunStream{
		Execution: execution,
	}); err != nil {
		return err
	}
	if jex == "" {
		return errors.New("grpc_agent: No executor defined, nothing to do")
	}

	// Check if executor exists
	// 找到对应的执行插件
	if executor, ok := as.agent.ExecutorPlugins[jex]; ok {
		as.logger.WithField("plugin", jex).Debug("grpc_agent: calling executor plugin")
		runningExecutions.Store(execution.GetGroup(), execution)
		// go-plugin grpc 调用执行
		out, err := executor.Execute(&types.ExecuteRequest{
			JobName: job.Name,
			Config:  exc,
			// callback, 将执行输出结果赋值到 output, 通过 stream 发送给服务端
			// ref: https://github.com/distribworks/dkron/pull/719
		}, &statusAgentHelper{
			stream:    stream,
			execution: execution,
		})

		if err == nil && out.Error != "" {
			err = errors.New(out.Error)
		}
		if err != nil {
			as.logger.WithError(err).WithField("job", job.Name).WithField("plugin", executor).Error("grpc_agent: command error output")
			success = false
			output.Write([]byte(err.Error() + "\n"))
		} else {
			success = true
		}

		if out != nil {
			output.Write(out.Output)
		}
	} else {
		as.logger.WithField("executor", jex).Error("grpc_agent: Specified executor is not present")
		output.Write([]byte("grpc_agent: Specified executor is not present"))
	}

	// 执行完成
	execution.FinishedAt = ptypes.TimestampNow()
	execution.Success = success
	execution.Output = output.Bytes()

	runningExecutions.Delete(execution.GetGroup())

	// 发送最终状态
	// Send the final execution
	if err := stream.Send(&types.AgentRunStream{
		Execution: execution,
	}); err != nil {
		// 有可能 server 没能接收到最后执行状态
		// In case of error means that maybe the server is gone so fallback to ExecutionDone
		as.logger.WithError(err).WithField("job", job.Name).Error("grpc_agent: error sending the final execution, falling back to ExecutionDone")
		// TCP 连接筛选一个 server
		rpcServer, err := as.agent.checkAndSelectServer()
		if err != nil {
			return err
		}
		// 调用执行完成
		return as.agent.GRPCClient.ExecutionDone(rpcServer, NewExecutionFromProto(execution))
	}

	return nil
}
