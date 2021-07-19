import json
import sys
import math
import copy


class TaskSetJsonKeys(object):
    # Task set
    KEY_TASKSET = "taskset"

    # Task
    KEY_TASK_ID = "taskId"
    KEY_TASK_PERIOD = "period"
    KEY_TASK_WCET = "wcet"
    KEY_TASK_DEADLINE = "deadline"
    KEY_TASK_OFFSET = "offset"
    KEY_TASK_SECTIONS = "sections"

    # Schedule
    KEY_SCHEDULE_START = "startTime"
    KEY_SCHEDULE_END = "endTime"

    # Release times
    KEY_RELEASETIMES = "releaseTimes"
    KEY_RELEASETIMES_JOBRELEASE = "timeInstant"
    KEY_RELEASETIMES_TASKID = "taskId"


class TaskSetIterator:
    def __init__(self, task_set):
        self.task_set = task_set
        self.index = 0
        self.keys = iter(task_set.tasks)

    def __next__(self):
        key = next(self.keys)
        return self.task_set.tasks[key]


class TaskSet(object):
    def __init__(self, data):
        self.parse_data_to_tasks(data)
        self.build_job_releases(data)

    def parse_data_to_tasks(self, data):
        task_set = {}

        for task_data in data[TaskSetJsonKeys.KEY_TASKSET]:
            task = Task(task_data)

            if task.id in task_set:
                print("Error: duplicate task ID: {0}".format(task.id))
                return

            if task.period < 0 and task.relative_deadline < 0:
                print("Error: aperiodic task must have positive relative deadline")
                return

            task_set[task.id] = task

        self.tasks = task_set

    def build_job_releases(self, data):
        jobs = []

        if TaskSetJsonKeys.KEY_RELEASETIMES in data:  # necessary for sporadic releases
            for jobRelease in data[TaskSetJsonKeys.KEY_RELEASETIMES]:
                release_time = float(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_JOBRELEASE])
                task_id = int(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_TASKID])

                job = self.get_task_by_id(task_id).spawn_job(release_time)
                jobs.append(job)
        else:
            schedule_start_time = float(data[TaskSetJsonKeys.KEY_SCHEDULE_START])
            schedule_end_time = float(data[TaskSetJsonKeys.KEY_SCHEDULE_END])
            for task in self:
                t = max(task.offset, schedule_start_time)
                while t < schedule_end_time:
                    job = task.spawn_job(t)
                    if job is not None:
                        jobs.append(job)

                    if task.period >= 0:
                        t += task.period  # periodic
                    else:
                        t = schedule_end_time  # aperiodic

        self.jobs = jobs

    def get_hyper_period(self):
        hyper_period = 1
        for task in self.tasks.values():
            hyper_period = hyper_period * int(task.period) // math.gcd(hyper_period, int(task.period))
        return hyper_period

    def is_feasible(self):
        u = [task.get_utilization() for task in self.tasks.values()]
        if sum(u) <= 1:
            if sum(u) <= len(self.tasks) * (2 ** len(self.tasks) - 1):
                return True
            p = 1
            for utilization in u:
                p *= (utilization + 1)
            if p <= 2:
                return True

            counter = 0
            periods = [task.period for task in self.tasks.values()]
            periods.sort()

            for period in periods:
                if period % periods[0] == 0:
                    counter += 1

            return counter == len(periods)
        return False

    def __contains__(self, elt):
        return elt in self.tasks

    def __iter__(self):
        return TaskSetIterator(self)

    def __len__(self):
        return len(self.tasks)

    def get_task_by_id(self, task_id):
        return self.tasks[task_id]

    def print_tasks(self):
        print("\nTask Set:")
        for task in self:
            print(task)

    def print_jobs(self):
        print("\nJobs:")
        for task in self:
            for job in task.get_jobs():
                print(job)


class Task(object):
    def __init__(self, task_dict):
        self.id = int(task_dict[TaskSetJsonKeys.KEY_TASK_ID])
        self.period = float(task_dict[TaskSetJsonKeys.KEY_TASK_PERIOD])
        self.wcet = float(task_dict[TaskSetJsonKeys.KEY_TASK_WCET])
        self.relative_deadline = float(
            task_dict.get(TaskSetJsonKeys.KEY_TASK_DEADLINE, task_dict[TaskSetJsonKeys.KEY_TASK_PERIOD]))
        self.offset = float(task_dict.get(TaskSetJsonKeys.KEY_TASK_OFFSET, 0.0))
        self.sections = task_dict[TaskSetJsonKeys.KEY_TASK_SECTIONS]

        self.last_job_id = 0
        self.last_released_time = 0.0

        self.jobs = []

    def get_all_resources(self):
        result = []
        for section in self.sections:
            if section[0] not in result and section[0] != 0:
                result.append(section[0])
        return result

    def spawn_job(self, release_time):
        if self.last_released_time > 0 and release_time < self.last_released_time:
            print("INVALID: release time of job is not monotonic")
            return None

        if self.last_released_time > 0 and release_time < self.last_released_time + self.period:
            print("INVALID: release times are not separated by period")
            return None

        self.last_job_id += 1
        self.last_released_time = release_time

        job = Job(self, self.last_job_id, release_time)

        self.jobs.append(job)
        return job

    def get_jobs(self):
        return self.jobs

    def get_job_by_id(self, job_id):
        if job_id > self.last_job_id:
            return None

        job = self.jobs[job_id - 1]
        if job.id == job_id:
            return job

        for job in self.jobs:
            if job.id == job_id:
                return job

        return None

    def get_utilization(self):
        return self.wcet / self.period

    def __str__(self):
        return "task {}: (Φ,T,C,D,∆) = ({}, {}, {}, {}, {})".format(self.id, self.offset, self.period, self.wcet,
                                                                    self.relative_deadline, self.sections)


class Job(object):
    def __init__(self, task, job_id, release_time):
        self.task = task
        self.id = job_id
        self.release_time = release_time
        self.deadline = release_time + task.relative_deadline
        self.is_active = False
        self.remaining_time = self.task.wcet

    def get_resource_held(self):
        temp = self.task.wcet
        for section in self.task.sections:
            temp -= section[1]
            if temp <= self.remaining_time:
                return section[0]
        return None

    def get_resource_waiting(self):
        '''a resource that is being waited on, but not currently executing'''
        pass

    def get_remaining_section_time(self):
        completed = 0
        for section in self.task.sections:
            completed += section[1]
            if completed >= self.task.wcet - self.remaining_time:
                return completed - self.task.wcet + self.remaining_time
        return 0

    def execute(self, time):
        execution_time = min(self.remaining_time, time)
        self.remaining_time -= execution_time
        return execution_time

    def execute_to_completion(self):
        return self.execute(self.release_time)

    def is_completed(self):
        return self.remaining_time == 0

    def __str__(self):
        return "[{0}:{1}] released at {2} -> deadline at {3}".format(self.task.id, self.id, self.release_time,
                                                                     self.deadline)


class RateMonotonic(object):
    def __init__(self, task_set):
        self.task_set = task_set
        self.hyper_period = self.task_set.get_hyper_period()

    def find_executing_task(self, task_set, t):
        task_id = -1
        period = self.hyper_period

        for job in task_set.jobs:
            if not job.is_completed():
                if period > job.task.period and t >= job.release_time:
                    task_id = job.task.id
                    period = job.task.period
        return task_id

    def run(self):
        task_set = copy.deepcopy(self.task_set)

        for t in range(self.hyper_period):
            executing_task = self.find_executing_task(task_set, t)
            if executing_task != -1:
                executing_job = task_set.get_task_by_id(executing_task).jobs[0]
                executing_job.execute(1)

                if executing_job.is_completed():
                    task_set.get_task_by_id(executing_task).jobs.pop(-1)

                print(f'Executing Task{executing_task} for 1 sec...')

            else:
                print('CPU IDLE for 1 sec...')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "taskset.json"

    with open(file_path) as json_data:
        data = json.load(json_data)

    task_set = TaskSet(data)

    task_set.print_tasks()
    task_set.print_jobs()

    rm = RateMonotonic(task_set)
    rm.run()