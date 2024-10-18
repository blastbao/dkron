package dkron

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dkronpb "github.com/distribworks/dkron/v3/plugin/types"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/buntdb"
)

const (
	// MaxExecutions to maintain in the storage
	MaxExecutions = 100

	jobsPrefix       = "jobs"
	executionsPrefix = "executions"
)

var (
	// ErrDependentJobs is returned when deleting a job that has dependent jobs
	ErrDependentJobs = errors.New("store: could not delete job with dependent jobs, delete childs first")
)

// Store is the local implementation of the Storage interface.
// It gives dkron the ability to manipulate its embedded storage
// BuntDB.
type Store struct {
	db   *buntdb.DB
	lock *sync.Mutex // for

	logger *logrus.Entry
}

// JobOptions additional options to apply when loading a Job.
type JobOptions struct {
	Metadata map[string]string `json:"tags"`
	Sort     string
	Order    string
	Query    string
	Status   string
	Disabled string
}

// ExecutionOptions additional options like "Sort" will be ready for JSON marshall
type ExecutionOptions struct {
	Sort     string
	Order    string
	Timezone *time.Location
}

type kv struct {
	Key   string
	Value []byte
}

// NewStore creates a new Storage instance.
// 创建基于内存的 buntdb
func NewStore(logger *logrus.Entry) (*Store, error) {
	db, err := buntdb.Open(":memory:")
	db.CreateIndex("name", jobsPrefix+":*", buntdb.IndexJSON("name"))                     // 任务名
	db.CreateIndex("started_at", executionsPrefix+":*", buntdb.IndexJSON("started_at"))   // 开始时间
	db.CreateIndex("finished_at", executionsPrefix+":*", buntdb.IndexJSON("finished_at")) // 结束时间
	db.CreateIndex("attempt", executionsPrefix+":*", buntdb.IndexJSON("attempt"))         // 尝试次数
	db.CreateIndex("displayname", jobsPrefix+":*", buntdb.IndexJSON("displayname"))       // 任务名
	db.CreateIndex("schedule", jobsPrefix+":*", buntdb.IndexJSON("schedule"))             // 调度
	db.CreateIndex("success_count", jobsPrefix+":*", buntdb.IndexJSON("success_count"))   // 成功数
	db.CreateIndex("error_count", jobsPrefix+":*", buntdb.IndexJSON("error_count"))       // 错误数
	db.CreateIndex("last_success", jobsPrefix+":*", buntdb.IndexJSON("last_success"))     // 上次成功
	db.CreateIndex("last_error", jobsPrefix+":*", buntdb.IndexJSON("last_error"))         // 上次失败
	db.CreateIndex("next", jobsPrefix+":*", buntdb.IndexJSON("next"))                     // 下次执行
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:     db,
		lock:   &sync.Mutex{},
		logger: logger,
	}

	return store, nil
}

// 储存 job
func (s *Store) setJobTxFunc(pbj *dkronpb.Job) func(tx *buntdb.Tx) error {
	return func(tx *buntdb.Tx) error {
		jobKey := fmt.Sprintf("%s:%s", jobsPrefix, pbj.Name)
		// json 序列化
		// 备注: 都已经是 proto struct 了, 序列化成 protobuf 不就好了吗?
		jb, err := json.Marshal(pbj)
		if err != nil {
			return err
		}
		s.logger.WithField("job", pbj.Name).Debug("store: Setting job")
		// 储存到 db
		if _, _, err := tx.Set(jobKey, string(jb), nil); err != nil {
			return err
		}
		return nil
	}
}

// DB is the getter for the BuntDB instance
func (s *Store) DB() *buntdb.DB {
	return s.db
}

// SetJob stores a job in the storage
func (s *Store) SetJob(job *Job, copyDependentJobs bool) error {
	// pbej 包含默认值
	var pbej dkronpb.Job
	var ej *Job

	// 校验 job 必要字段
	if err := job.Validate(); err != nil {
		return err
	}

	// Abort if parent not found before committing job to the store
	if job.ParentJob != "" {
		if j, _ := s.GetJob(job.ParentJob, nil); j == nil {
			return ErrParentJobNotFound
		}
	}

	err := s.db.Update(func(tx *buntdb.Tx) error {
		// Get if the requested job already exist
		// 根据 job name 读取 store ，拿到 *proto.Job 数据后反序列化到 &pbej 上
		err := s.getJobTxFunc(job.Name, &pbej)(tx)
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}

		// 把 *proto.Job 转换为 *Job 对象
		ej = NewJobFromProto(&pbej)

		// 不存在的话 name 为空字符串，否则将 ej 中一些参数填入 job 中
		if ej.Name != "" {
			// 填充 job 运行时 metadata
			// When the job runs, these status vars are updated
			// otherwise use the ones that are stored
			if ej.LastError.After(job.LastError) {
				job.LastError = ej.LastError
			}
			if ej.LastSuccess.After(job.LastSuccess) {
				job.LastSuccess = ej.LastSuccess
			}
			if ej.SuccessCount > job.SuccessCount {
				job.SuccessCount = ej.SuccessCount
			}
			if ej.ErrorCount > job.ErrorCount {
				job.ErrorCount = ej.ErrorCount
			}
			if len(ej.DependentJobs) != 0 && copyDependentJobs {
				job.DependentJobs = ej.DependentJobs
			}
			if ej.Status != "" {
				job.Status = ej.Status
			}
		}

		if job.Schedule != ej.Schedule {
			job.Next, err = job.GetNext()
			if err != nil {
				return err
			}
		} else {
			// If comming from a backup us the previous value, don't allow overwriting this
			if job.Next.Before(ej.Next) {
				job.Next = ej.Next
			}
		}

		// 把 *Job 转换为 *proto.Job 对象
		pbj := job.ToProto()

		// 将 *proto.Job 对象序列化后存储到 store 中，key = "jobs:{job_name}"
		s.setJobTxFunc(pbj)(tx)
		return nil
	})
	if err != nil {
		return err
	}

	// If the parent job changed update the parents of the old (if any) and new jobs
	// 如果父作业发生了变化，请更新旧作业（如果有的话）和新作业的父作业。
	if job.ParentJob != ej.ParentJob {
		if err := s.removeFromParent(ej); err != nil {
			return err
		}
		if err := s.addToParent(job); err != nil {
			return err
		}
	}

	return nil
}

// Removes the given job from its parent.
// Does nothing if nil is passed as child.
func (s *Store) removeFromParent(child *Job) error {
	// Do nothing if no job was given or job has no parent
	if child == nil || child.ParentJob == "" {
		return nil
	}

	parent, err := child.GetParent(s)
	if err != nil {
		return err
	}

	// Remove all occurrences from the parent, not just one.
	// Due to an old bug (in v1), a parent can have the same child more than once.
	djs := []string{}
	for _, djn := range parent.DependentJobs {
		if djn != child.Name {
			djs = append(djs, djn)
		}
	}
	parent.DependentJobs = djs
	if err := s.SetJob(parent, false); err != nil {
		return err
	}

	return nil
}

// Adds the given job to its parent.
func (s *Store) addToParent(child *Job) error {
	// Do nothing if job has no parent
	if child.ParentJob == "" {
		return nil
	}

	parent, err := child.GetParent(s)
	if err != nil {
		return err
	}

	parent.DependentJobs = append(parent.DependentJobs, child.Name)
	if err := s.SetJob(parent, false); err != nil {
		return err
	}

	return nil
}

// SetExecutionDone saves the execution and updates the job with the corresponding
// results
func (s *Store) SetExecutionDone(execution *Execution) (bool, error) {
	err := s.db.Update(func(tx *buntdb.Tx) error {
		// Load the job from the store
		// 从 db 中查找 job
		var pbj dkronpb.Job
		if err := s.getJobTxFunc(execution.JobName, &pbj)(tx); err != nil {
			if err == buntdb.ErrNotFound {
				s.logger.Warn(ErrExecutionDoneForDeletedJob)
				return ErrExecutionDoneForDeletedJob
			}
			s.logger.WithError(err).Fatal(err)
			return err
		}

		// 任务每次执行有唯一的 execution id
		key := fmt.Sprintf("%s:%s:%s", executionsPrefix, execution.JobName, execution.Key())

		// Save the execution to store
		// 把 execution 保存到 db
		pbe := execution.ToProto()
		if err := s.setExecutionTxFunc(key, pbe)(tx); err != nil {
			return err
		}

		// 更新 job 的执行成功数/成功时间、失败数/失败时间、...
		if pbe.Success {
			pbj.LastSuccess.HasValue = true
			pbj.LastSuccess.Time = pbe.FinishedAt
			pbj.SuccessCount++
		} else {
			pbj.LastError.HasValue = true
			pbj.LastError.Time = pbe.FinishedAt
			pbj.ErrorCount++
		}

		// 更新 job 的状态
		status, err := s.computeStatus(pbj.Name, pbe.Group, tx)
		if err != nil {
			return err
		}
		pbj.Status = status

		// 向 db 中写入(更新) job
		if err := s.setJobTxFunc(&pbj)(tx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		s.logger.WithError(err).Error("store: Error in SetExecutionDone")
		return false, err
	}

	return true, nil
}

func (s *Store) jobHasMetadata(job *Job, metadata map[string]string) bool {
	if job == nil || job.Metadata == nil || len(job.Metadata) == 0 {
		return false
	}

	for k, v := range metadata {
		if val, ok := job.Metadata[k]; !ok || v != val {
			return false
		}
	}

	return true
}

// GetJobs returns all jobs
func (s *Store) GetJobs(options *JobOptions) ([]*Job, error) {
	// 默认按照 name 对 jobs 排序
	if options == nil {
		options = &JobOptions{
			Sort: "name",
		}
	}

	// 结果集合 + 查找函数
	jobs := make([]*Job, 0)
	jobsFn := func(key, item string) bool {
		// 反序列化: 得到 job pb
		var pbj dkronpb.Job
		// [TODO] This condition is temporary while we migrate to JSON marshalling for jobs
		// so we can use BuntDb indexes. To be removed in future versions.
		if err := proto.Unmarshal([]byte(item), &pbj); err != nil {
			if err := json.Unmarshal([]byte(item), &pbj); err != nil {
				return false
			}
		}
		// 格式转换: pb => model
		job := NewJobFromProto(&pbj)
		job.logger = s.logger
		// 条件匹配: 添加到结果集
		if options == nil ||
			(options.Metadata == nil || len(options.Metadata) == 0 || s.jobHasMetadata(job, options.Metadata)) &&
				(options.Query == "" || strings.Contains(job.Name, options.Query) || strings.Contains(job.DisplayName, options.Query)) &&
				(options.Disabled == "" || strconv.FormatBool(job.Disabled) == options.Disabled) &&
				((options.Status == "untriggered" && job.Status == "") || (options.Status == "" || job.Status == options.Status)) {
			jobs = append(jobs, job)
		}
		return true
	}

	// 执行查询
	err := s.db.View(func(tx *buntdb.Tx) error {
		var err error
		if options.Order == "DESC" {
			err = tx.Descend(options.Sort, jobsFn)
		} else {
			err = tx.Ascend(options.Sort, jobsFn)
		}
		return err
	})

	return jobs, err
}

// GetJob finds and return a Job from the store
func (s *Store) GetJob(name string, options *JobOptions) (*Job, error) {
	var pbj dkronpb.Job

	err := s.db.View(s.getJobTxFunc(name, &pbj))
	if err != nil {
		return nil, err
	}

	job := NewJobFromProto(&pbj)
	job.logger = s.logger

	return job, nil
}

// This will allow reuse this code to avoid nesting transactions
// 返回 func 接收 tx 作为参数, 复用一个事务
func (s *Store) getJobTxFunc(name string, pbj *dkronpb.Job) func(tx *buntdb.Tx) error {
	return func(tx *buntdb.Tx) error {
		item, err := tx.Get(fmt.Sprintf("%s:%s", jobsPrefix, name))
		if err != nil {
			return err
		}
		// [TODO] This condition is temporary while we migrate to JSON marshalling for jobs
		// so we can use BuntDb indexes. To be removed in future versions.
		if err := proto.Unmarshal([]byte(item), pbj); err != nil {
			if err := json.Unmarshal([]byte(item), pbj); err != nil {
				return err
			}
		}
		s.logger.WithFields(logrus.Fields{
			"job": pbj.Name,
		}).Debug("store: Retrieved job from datastore")
		return nil
	}
}

// DeleteJob deletes the given job from the store, along with
// all its executions and references to it.
func (s *Store) DeleteJob(name string) (*Job, error) {
	var job *Job
	err := s.db.Update(func(tx *buntdb.Tx) error {
		// Get the job
		var pbj dkronpb.Job
		if err := s.getJobTxFunc(name, &pbj)(tx); err != nil {
			return err
		}
		// Check if the job has dependent jobs and return an error indicating to remove childs first.
		if len(pbj.DependentJobs) > 0 {
			return ErrDependentJobs
		}
		job = NewJobFromProto(&pbj)
		if err := s.deleteExecutionsTxFunc(name)(tx); err != nil {
			return err
		}
		_, err := tx.Delete(fmt.Sprintf("%s:%s", jobsPrefix, name))
		return err
	})
	if err != nil {
		return nil, err
	}

	// If the transaction succeded, remove from parent
	if job.ParentJob != "" {
		if err := s.removeFromParent(job); err != nil {
			return nil, err
		}
	}

	return job, nil
}

// GetExecutions returns the executions given a Job name.
func (s *Store) GetExecutions(jobName string, opts *ExecutionOptions) ([]*Execution, error) {
	prefix := fmt.Sprintf("%s:%s:", executionsPrefix, jobName)

	kvs, err := s.list(prefix, true, opts)
	if err != nil {
		return nil, err
	}

	return s.unmarshalExecutions(kvs, opts.Timezone)
}

func (s *Store) list(prefix string, checkRoot bool, opts *ExecutionOptions) ([]kv, error) {
	var found bool
	kvs := []kv{}

	err := s.db.View(s.listTxFunc(prefix, &kvs, &found, opts))
	if err == nil && !found && checkRoot {
		return nil, buntdb.ErrNotFound
	}

	return kvs, err
}

func (*Store) listTxFunc(prefix string, kvs *[]kv, found *bool, opts *ExecutionOptions) func(tx *buntdb.Tx) error {
	fnc := func(key, value string) bool {
		if strings.HasPrefix(key, prefix) {
			*found = true
			// ignore self in listing
			if !bytes.Equal(trimDirectoryKey([]byte(key)), []byte(prefix)) {
				kv := kv{Key: key, Value: []byte(value)}
				*kvs = append(*kvs, kv)
			}
		}
		return true
	}

	return func(tx *buntdb.Tx) (err error) {
		if opts.Order == "DESC" {
			err = tx.Descend(opts.Sort, fnc)
		} else {
			err = tx.Ascend(opts.Sort, fnc)
		}
		return err
	}
}

// GetExecutionGroup returns all executions in the same group of a given execution
func (s *Store) GetExecutionGroup(execution *Execution, opts *ExecutionOptions) ([]*Execution, error) {
	// 查询 job 下所有 executions
	res, err := s.GetExecutions(execution.JobName, opts)
	if err != nil {
		return nil, err
	}
	// 提取出属于 Group 的那些 executions
	var executions []*Execution
	for _, ex := range res {
		if ex.Group == execution.Group {
			executions = append(executions, ex)
		}
	}
	// 返回
	return executions, nil
}

// GetGroupedExecutions returns executions for a job grouped and with an ordered index
// to facilitate access.
func (s *Store) GetGroupedExecutions(jobName string, opts *ExecutionOptions) (map[int64][]*Execution, []int64, error) {
	execs, err := s.GetExecutions(jobName, opts)
	if err != nil {
		return nil, nil, err
	}
	groups := make(map[int64][]*Execution)
	for _, exec := range execs {
		groups[exec.Group] = append(groups[exec.Group], exec)
	}

	// Build a separate data structure to show in order
	var byGroup int64arr
	for key := range groups {
		byGroup = append(byGroup, key)
	}
	sort.Sort(sort.Reverse(byGroup))

	return groups, byGroup, nil
}

func (*Store) setExecutionTxFunc(key string, pbe *dkronpb.Execution) func(tx *buntdb.Tx) error {
	return func(tx *buntdb.Tx) error {
		// Get previous execution
		i, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		// Do nothing if a previous execution exists and is
		// more recent, avoiding non ordered execution set
		// 已经存在？
		if i != "" {
			var p dkronpb.Execution
			// [TODO] This condition is temporary while we migrate to JSON marshalling for executions
			// so we can use BuntDb indexes. To be removed in future versions.
			if err := proto.Unmarshal([]byte(i), &p); err != nil {
				if err := json.Unmarshal([]byte(i), &p); err != nil {
					return err
				}
			}
			// Compare existing execution
			if p.GetFinishedAt().Seconds > pbe.GetFinishedAt().Seconds {
				return nil
			}
		}

		// 序列化
		eb, err := json.Marshal(pbe)
		if err != nil {
			return err
		}

		// 保存
		_, _, err = tx.Set(key, string(eb), nil)
		return err
	}
}

// SetExecution Save a new execution and returns the key of the new saved item or an error.
func (s *Store) SetExecution(execution *Execution) (string, error) {
	pbe := execution.ToProto()
	key := fmt.Sprintf("%s:%s:%s", executionsPrefix, execution.JobName, execution.Key())

	s.logger.WithFields(logrus.Fields{
		"job":       execution.JobName,
		"execution": key,
		"finished":  execution.FinishedAt.String(),
	}).Debug("store: Setting key")

	// 保存到数据库
	err := s.db.Update(s.setExecutionTxFunc(key, pbe))
	if err != nil {
		s.logger.WithError(err).WithFields(logrus.Fields{
			"job":       execution.JobName,
			"execution": key,
		}).Debug("store: Failed to set key")
		return "", err
	}

	// 获取作业的所有执行记录
	execs, err := s.GetExecutions(execution.JobName, &ExecutionOptions{})
	if err != nil && !errors.Is(err, buntdb.ErrNotFound) {
		s.logger.WithError(err).WithField("job", execution.JobName).
			Error("store: Error getting executions for job")
	}

	// Delete all execution results over the limit, starting from olders
	//
	// 如果执行记录的数量超过了 MaxExecutions，则按 StartedAt（开始时间）对记录进行排序，从最早的记录开始删除。
	if len(execs) > MaxExecutions {
		//sort the array of all execution groups by StartedAt time
		sort.Slice(execs, func(i, j int) bool {
			return execs[i].StartedAt.Before(execs[j].StartedAt)
		})
		for i := 0; i < len(execs)-MaxExecutions; i++ {
			s.logger.WithFields(logrus.Fields{
				"job":       execs[i].JobName,
				"execution": execs[i].Key(),
			}).Debug("store: to detele key")
			err = s.db.Update(func(tx *buntdb.Tx) error {
				k := fmt.Sprintf("%s:%s:%s", executionsPrefix, execs[i].JobName, execs[i].Key())
				_, err := tx.Delete(k)
				return err
			})
			if err != nil {
				s.logger.WithError(err).WithField("execution", execs[i].Key()).
					Error("store: Error trying to delete overflowed execution")
			}
		}
	}

	return key, nil
}

// DeleteExecutions removes all executions of a job
func (s *Store) deleteExecutionsTxFunc(jobName string) func(tx *buntdb.Tx) error {
	return func(tx *buntdb.Tx) error {
		var delkeys []string
		prefix := fmt.Sprintf("%s:%s", executionsPrefix, jobName)
		tx.Ascend("", func(key, value string) bool {
			if strings.HasPrefix(key, prefix) {
				delkeys = append(delkeys, key)
			}
			return true
		})

		for _, k := range delkeys {
			_, _ = tx.Delete(k)
		}

		return nil
	}
}

// Shutdown close the KV store
func (s *Store) Shutdown() error {
	return s.db.Close()
}

// Snapshot creates a backup of the data stored in BuntDB
func (s *Store) Snapshot(w io.WriteCloser) error {
	// Save 负责将数据库的快照写入到 io.Writer 中，用于备份。
	// Save 会阻塞写操作，但不会影响读取。
	return s.db.Save(w)
}

// Restore load data created with backup in to Bunt
func (s *Store) Restore(r io.ReadCloser) error {
	return s.db.Load(r)
}

func (s *Store) unmarshalExecutions(items []kv, timezone *time.Location) ([]*Execution, error) {
	var executions []*Execution
	for _, item := range items {
		var pbe dkronpb.Execution

		// [TODO] This condition is temporary while we migrate to JSON marshalling for jobs
		// so we can use BuntDb indexes. To be removed in future versions.
		if err := proto.Unmarshal([]byte(item.Value), &pbe); err != nil {
			if err := json.Unmarshal(item.Value, &pbe); err != nil {
				s.logger.WithError(err).WithField("key", item.Key).Debug("error unmarshaling JSON")
				return nil, err
			}
		}
		execution := NewExecutionFromProto(&pbe)
		if timezone != nil {
			execution.FinishedAt = execution.FinishedAt.In(timezone)
			execution.StartedAt = execution.StartedAt.In(timezone)
		}
		executions = append(executions, execution)
	}
	return executions, nil
}

func (s *Store) computeStatus(jobName string, exGroup int64, tx *buntdb.Tx) (string, error) {
	// compute job status based on execution group
	kvs := []kv{}
	found := false
	prefix := fmt.Sprintf("%s:%s:", executionsPrefix, jobName)

	if err := s.listTxFunc(prefix, &kvs, &found, &ExecutionOptions{})(tx); err != nil {
		return "", err
	}

	execs, err := s.unmarshalExecutions(kvs, nil)
	if err != nil {
		return "", err
	}

	var executions []*Execution
	for _, ex := range execs {
		if ex.Group == exGroup {
			executions = append(executions, ex)
		}
	}

	success := 0
	failed := 0

	var status string
	for _, ex := range executions {
		if ex.Success {
			success = success + 1
		} else {
			failed = failed + 1
		}
	}

	if failed == 0 {
		status = StatusSuccess
	} else if failed > 0 && success == 0 {
		status = StatusFailed
	} else if failed > 0 && success > 0 {
		status = StatusPartialyFailed
	}

	return status, nil
}

func trimDirectoryKey(key []byte) []byte {
	if isDirectoryKey(key) {
		return key[:len(key)-1]
	}

	return key
}

func isDirectoryKey(key []byte) bool {
	return len(key) > 0 && key[len(key)-1] == ':'
}
