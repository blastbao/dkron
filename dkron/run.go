package dkron

import (
	"fmt"
	"sync"

	"github.com/hashicorp/serf/serf"
)

// Run call the agents to run a job. Returns a job with it's new status and next schedule.
//
// [重要]
// 调度运行 Job -> 分发到 agent 执行任务
//
// - 从 store 获取任务详情
// - 计算下一次执行时间，然后更新任务到 store
func (a *Agent) Run(jobName string, ex *Execution) (*Job, error) {
	job, err := a.Store.GetJob(jobName, nil)
	if err != nil {
		return nil, fmt.Errorf("agent: Run error retrieving job: %s from store: %w", jobName, err)
	}

	// In case the job is not a child job, compute the next execution time
	if job.ParentJob == "" {
		// 获取 cron.Entry
		if e, ok := a.sched.GetEntry(jobName); ok {
			// 获取下一次执行时间
			job.Next = e.Next
			// 同步 job 数据到 raft store
			if err := a.applySetJob(job.ToProto()); err != nil {
				return nil, fmt.Errorf("agent: Run error storing job %s before running: %w", jobName, err)
			}
		} else {
			return nil, fmt.Errorf("agent: Run error retrieving job: %s from scheduler", jobName)
		}
	}

	// In the first execution attempt we build and filter the target nodes
	// but we use the existing node target in case of retry.
	// 在第一次执行尝试时，我们会构建并过滤目标节点，但是在重试的情况下，会使用现有的节点目标。
	var filterMap map[string]string
	if ex.Attempt <= 1 {
		filterMap, _, err = a.processFilteredNodes(job)
		if err != nil {
			return nil, fmt.Errorf("run error processing filtered nodes: %w", err)
		}
	} else {
		// In case of retrying, find the rpc address of the node or return with an error
		// 重试使用同样的 Node 执行
		var addr string
		for _, m := range a.serf.Members() {
			if ex.NodeName == m.Name {
				if m.Status == serf.StatusAlive {
					addr = m.Tags["rpc_addr"]
				} else {
					return nil, fmt.Errorf("retry node is gone: %s for job %s", ex.NodeName, ex.JobName)
				}
			}
		}
		filterMap = map[string]string{ex.NodeName: addr}
	}

	// In case no nodes found, return reporting the error
	if len(filterMap) < 1 {
		return nil, fmt.Errorf("no target nodes found to run job %s", ex.JobName)
	}
	a.logger.WithField("nodes", filterMap).Debug("agent: Filtered nodes to run")

	var wg sync.WaitGroup
	for _, v := range filterMap {
		// Call here client GRPC AgentRun
		wg.Add(1)
		go func(node string, wg *sync.WaitGroup) {
			defer wg.Done()
			a.logger.WithFields(map[string]interface{}{
				"job_name": job.Name,
				"node":     node,
			}).Info("agent: Calling AgentRun")

			err := a.GRPCClient.AgentRun(node, job.ToProto(), ex.ToProto())
			if err != nil {
				a.logger.WithFields(map[string]interface{}{
					"job_name": job.Name,
					"node":     node,
				}).Error("agent: Error calling AgentRun")
			}
		}(v, &wg)
	}

	// 等待所有节点执行完
	wg.Wait()
	return job, nil
}
