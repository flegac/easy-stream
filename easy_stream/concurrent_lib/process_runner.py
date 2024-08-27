import multiprocessing
import os
import queue
import threading
import time
import traceback

from easy_stream.concurrent_lib.runner import Runner
from easy_stream.concurrent_lib.task.parallel import Parallel
from easy_stream.concurrent_lib.task.serial import Serial
from easy_stream.concurrent_lib.task.task import Task
from easy_kit.timing import time_func

RETRY_SLEEP_TIME = .01
QUEUE_TIMEOUT = .05


class ProcessRunner(Runner):
    def __init__(self, worker_count: int = None):
        self.worker_count = worker_count or 2 + os.cpu_count()
        manager = multiprocessing.Manager()
        self._scheduled = manager.Queue()
        self._waiting = manager.list()
        self._completed = manager.list()
        self._hierarchy = manager.list()

    def completed_tasks(self) -> int:
        return len(self._completed) - len(self._hierarchy)

    @time_func
    def run(self, task: Task):
        self._enqueue(task)
        # with multiprocessing.get_context("spawn").Pool() as pool:
        with multiprocessing.Pool(processes=self.worker_count) as pool:
            pool.map(self._worker, [_ for _ in range(self.worker_count)])

    def _worker(self, idx: int):
        try:
            if idx == 0:
                self._waiting_thread()
            else:
                self._worker_thread()
        except:
            print(f'{threading.currentThread()}: {traceback.format_exc()}')
        finally:
            # print(f'stopping({idx})')
            pass

    def _waiting_thread(self):
        retries = initial_retries = 3
        while retries > 0:
            for t in set(self._waiting):
                unfinished = set(t.tasks).difference(self._completed)
                if len(unfinished) == 0:
                    self._waiting.remove(t)
                    retries = initial_retries
            if self.is_done():
                retries -= 1
                time.sleep(10 * RETRY_SLEEP_TIME)

    def _worker_thread(self):
        retries = initial_retries = 3
        while retries > 0:
            try:
                task = self._scheduled.get(block=True, timeout=QUEUE_TIMEOUT)
                self.run_any(task)
                self._scheduled.task_done()
                retries = initial_retries
            except queue.Empty:
                retries -= 1
                time.sleep(RETRY_SLEEP_TIME)

    def is_done(self):
        return len(self._waiting) == 0 and self._scheduled.empty()

    def parallel(self, task: Parallel):
        # tasks = list(sorted(task.tasks, key=lambda t: t.task_size, reverse=True))
        # for t in tasks:
        for t in task.tasks:
            self._enqueue(t)

        self._wait_completion(task)
        self._hierarchy.append(task)
        self._completed.append(task)

    def serial(self, task: Serial):
        first, remaining = task.split()
        self.run_any(first)
        if len(remaining.tasks) == 0:
            self._hierarchy.append(task)
            self._completed.append(task)
        else:
            self._enqueue(remaining)

    def task(self, task: Task):
        task.runner()
        self._completed.append(task)

    def _enqueue(self, task: Task):
        self._scheduled.put(task, block=True, timeout=QUEUE_TIMEOUT)

    def _wait_completion(self, task: Parallel):
        self._waiting.append(task)
