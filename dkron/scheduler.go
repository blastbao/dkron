package dkron

import (
	"errors"
	"expvar"
	"strings"
	"sync"

	"github.com/armon/go-metrics"
	"github.com/distribworks/dkron/v3/extcron"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var (
	cronInspect      = expvar.NewMap("cron_entries")
	schedulerStarted = expvar.NewInt("scheduler_started")

	// ErrScheduleParse is the error returned when the schdule parsing fails.
	ErrScheduleParse = errors.New("can't parse job schedule")
)

// Scheduler represents a dkron scheduler instance, it stores the cron engine
// and the related parameters.
type Scheduler struct {
	Cron        *cron.Cron
	Started     bool
	EntryJobMap sync.Map
	logger      *logrus.Entry
}

// NewScheduler creates a new Scheduler instance
// 创建调度器
func NewScheduler(logger *logrus.Entry) *Scheduler {
	schedulerStarted.Set(0)
	return &Scheduler{
		Cron:        nil,
		Started:     false,
		EntryJobMap: sync.Map{},
		logger:      logger,
	}
}

// Start the cron scheduler, adding its corresponding jobs and
// executing them on time.
// 启动调度器：遍历 jobs ，挨个设置 job.Agent ，然后添加到 Scheduler 中，之后执行 Scheduler.Cron.Start() ；
func (s *Scheduler) Start(jobs []*Job, agent *Agent) error {
	// 创建 cron
	s.Cron = cron.New(cron.WithParser(extcron.NewParser()))

	metrics.IncrCounter([]string{"scheduler", "start"}, 1)
	for _, job := range jobs {
		job.Agent = agent
		// 添加所有 Job 到 cron 中
		s.AddJob(job)
	}

	// 启动 cron
	s.Cron.Start()
	s.Started = true
	schedulerStarted.Set(1)
	return nil
}

// Stop stops the scheduler effectively not running any job.
func (s *Scheduler) Stop() {
	if s.Started {
		s.logger.Debug("scheduler: Stopping scheduler")
		s.Cron.Stop()
		s.Started = false
		// Keep Cron exists and let the jobs which have been scheduled can continue to finish,
		// even the node's leadership will be revoked.
		// Ignore the running jobs and make s.Cron to nil may cause whole process crashed.
		//s.Cron = nil

		// expvars
		cronInspect.Do(func(kv expvar.KeyValue) {
			kv.Value = nil
		})
	}
	schedulerStarted.Set(0)
}

// Restart the scheduler
func (s *Scheduler) Restart(jobs []*Job, agent *Agent) {
	s.Stop()
	s.ClearCron()
	s.Start(jobs, agent)
}

// Clear cron separately, this can only be called when agent will be stop.
func (s *Scheduler) ClearCron() {
	s.Cron = nil
}

// GetEntry returns a scheduler entry from a snapshot in
// the current time, and whether or not the entry was found.
func (s *Scheduler) GetEntry(jobName string) (cron.Entry, bool) {
	for _, e := range s.Cron.Entries() {
		j, _ := e.Job.(*Job)
		j.logger = s.logger
		if j.Name == jobName {
			return e, true
		}
	}
	return cron.Entry{}, false
}

// AddJob Adds a job to the cron scheduler
// 调度器添加 Job
func (s *Scheduler) AddJob(job *Job) error {
	// Check if the job is already set and remove it if exists
	// 已存在同名任务，先移除
	if _, ok := s.EntryJobMap.Load(job.Name); ok {
		s.RemoveJob(job)
	}

	if job.Disabled || job.ParentJob != "" {
		return nil
	}

	s.logger.WithFields(logrus.Fields{
		"job": job.Name,
	}).Debug("scheduler: Adding job to cron")

	// If Timezone is set on the job, and not explicitly in its schedule,
	// AND its not a descriptor (that don't support timezones), add the
	// timezone to the schedule so robfig/cron knows about it.
	schedule := job.Schedule
	if job.Timezone != "" &&
		!strings.HasPrefix(schedule, "@") &&
		!strings.HasPrefix(schedule, "TZ=") &&
		!strings.HasPrefix(schedule, "CRON_TZ=") {
		schedule = "CRON_TZ=" + job.Timezone + " " + schedule
	}

	// 为 cron 添加一个 job ，当定时器触发时会回调 Job.Run() 方法。
	id, err := s.Cron.AddJob(schedule, job)
	if err != nil {
		return err
	}
	// 储存 job 的 id
	s.EntryJobMap.Store(job.Name, id)

	// expvar
	cronInspect.Set(job.Name, job)
	metrics.IncrCounterWithLabels([]string{"scheduler", "job_add"}, 1, []metrics.Label{{Name: "job", Value: job.Name}})

	return nil
}

// RemoveJob removes a job from the cron scheduler
func (s *Scheduler) RemoveJob(job *Job) {
	s.logger.WithFields(logrus.Fields{
		"job": job.Name,
	}).Debug("scheduler: Removing job from cron")
	if v, ok := s.EntryJobMap.Load(job.Name); ok {
		s.Cron.Remove(v.(cron.EntryID))
		s.EntryJobMap.Delete(job.Name)

		cronInspect.Delete(job.Name)
		metrics.IncrCounterWithLabels([]string{"scheduler", "job_delete"}, 1, []metrics.Label{{Name: "job", Value: job.Name}})
	}
}
