package dkron

import (
	"io"

	dkronpb "github.com/distribworks/dkron/v3/plugin/types"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

// MessageType is the type to encode FSM commands.
type MessageType uint8

const (
	// SetJobType is the command used to store a job in the store.
	SetJobType MessageType = iota
	// DeleteJobType is the command used to delete a Job from the store.
	DeleteJobType
	// SetExecutionType is the command used to store an Execution to the store.
	SetExecutionType
	// DeleteExecutionsType is the command used to delete executions from the store.
	DeleteExecutionsType
	// ExecutionDoneType is the command to perform the logic needed once an exeuction
	// is done.
	ExecutionDoneType
)

// LogApplier is the definition of a function that can apply a Raft log
type LogApplier func(buf []byte, index uint64) interface{}

// LogAppliers is a mapping of the Raft MessageType to the appropriate log
// applier
type LogAppliers map[MessageType]LogApplier

type dkronFSM struct {
	store Storage

	// proAppliers holds the set of pro only LogAppliers
	proAppliers LogAppliers
	logger      *logrus.Entry
}

// NewFSM is used to construct a new FSM with a blank state
// 实现 raft.FSM
func newFSM(store Storage, logAppliers LogAppliers, logger *logrus.Entry) *dkronFSM {
	return &dkronFSM{
		store:       store,
		proAppliers: logAppliers,
		logger:      logger,
	}
}

// Apply applies a Raft log entry to the key-value store.
// 创建/同步一条 Log 到 raft state machine
func (d *dkronFSM) Apply(l *raft.Log) interface{} {
	buf := l.Data
	msgType := MessageType(buf[0])

	d.logger.WithField("command", msgType).Debug("fsm: received command")

	switch msgType {
	case SetJobType:
		return d.applySetJob(buf[1:]) // 保存任务
	case DeleteJobType:
		return d.applyDeleteJob(buf[1:]) // 删除任务
	case ExecutionDoneType:
		return d.applyExecutionDone(buf[1:]) // 执行完毕
	case SetExecutionType:
		return d.applySetExecution(buf[1:]) // 执行类型
	}

	// Check enterprise only message types.
	// 其它类型的消息，可以走自定义逻辑
	if applier, ok := d.proAppliers[msgType]; ok {
		return applier(buf[1:], l.Index)
	}

	return nil
}

func (d *dkronFSM) applySetJob(buf []byte) interface{} {
	// 反序列化 []byte => protobuf
	var pj dkronpb.Job
	if err := proto.Unmarshal(buf, &pj); err != nil {
		return err
	}
	// 字段映射 protobuf => golang struct
	job := NewJobFromProto(&pj)
	// 存储到 store
	if err := d.store.SetJob(job, false); err != nil {
		return err
	}
	return nil
}

func (d *dkronFSM) applyDeleteJob(buf []byte) interface{} {
	// 反序列化 []byte => protobuf
	var djr dkronpb.DeleteJobRequest
	if err := proto.Unmarshal(buf, &djr); err != nil {
		return err
	}
	// 从 store 中删除
	job, err := d.store.DeleteJob(djr.GetJobName())
	if err != nil {
		return err
	}
	return job
}

func (d *dkronFSM) applyExecutionDone(buf []byte) interface{} {
	// 反序列化 []byte => protobuf
	var execDoneReq dkronpb.ExecutionDoneRequest
	if err := proto.Unmarshal(buf, &execDoneReq); err != nil {
		return err
	}
	// 转换 pb => model
	execution := NewExecutionFromProto(execDoneReq.Execution)
	d.logger.WithField("execution", execution.Key()).
		WithField("output", string(execution.Output)).
		Debug("fsm: Setting execution")
	// 保存到 store
	_, err := d.store.SetExecutionDone(execution)
	return err
}

func (d *dkronFSM) applySetExecution(buf []byte) interface{} {
	var pbex dkronpb.Execution
	if err := proto.Unmarshal(buf, &pbex); err != nil {
		return err
	}
	execution := NewExecutionFromProto(&pbex)
	key, err := d.store.SetExecution(execution)
	if err != nil {
		return err
	}
	return key
}

// Snapshot returns a snapshot of the key-value store. We wrap
// the things we need in dkronSnapshot and then send that over to Persist.
// Persist encodes the needed data from dkronSnapshot and transport it to
// Restore where the necessary data is replicated into the finite state machine.
// This allows the consensus algorithm to truncate the replicated log.
//
// Snapshot 返回 fsm 的快照，本质就是取 store 的快照
func (d *dkronFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &dkronSnapshot{store: d.store}, nil
}

// Restore stores the key-value store to a previous state.
//
// Restore 从快照中恢复 fsm ，本质上是在恢复 store
func (d *dkronFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	return d.store.Restore(r)
}

type dkronSnapshot struct {
	store Storage
}

func (d *dkronSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := d.store.Snapshot(sink); err != nil {
		sink.Cancel()
		return err
	}
	// Close the sink.
	if err := sink.Close(); err != nil {
		return err
	}
	return nil
}

func (d *dkronSnapshot) Release() {}
