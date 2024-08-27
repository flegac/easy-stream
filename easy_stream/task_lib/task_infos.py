import json
import os
from enum import Enum, auto

from easy_stream.task_lib.context import Context


class TaskStatus(Enum):
    Pending = auto()
    Running = auto()
    Done = auto()
    Error = auto()


class TaskInfos:
    def __init__(self, name: str):
        self.name = name
        self.status = TaskStatus.Pending
        self.duration = 0.
        self.disk_usage = 0
        self.file_count = 0
        self.error = None

    @staticmethod
    def from_context(ctx: Context):
        path = ctx.path_to('.status.json')
        if not path.exists():
            return TaskInfos(ctx.step_id)

        with path.open() as _:
            return TaskInfos.from_dict(json.load(_))

    def save(self, ctx: Context):
        disk_usage = 0
        file_count = len(list(ctx.output.glob('*'))) - 1
        for p in ctx.output.glob('**/*'):
            disk_usage += os.path.getsize(p)
        self.disk_usage = disk_usage
        self.file_count = file_count
        path = ctx.path_to('.status.json')
        with path.open('w') as _:
            json.dump(self.to_dict(), _, indent=4, sort_keys=True)

    @staticmethod
    def from_dict(data: dict):
        infos = TaskInfos(data['name'])
        infos.status = TaskStatus[data['status']]
        infos.duration = data.get('duration', 0.)
        infos.disk_usage = data.get('disk_usage', 0)
        infos.file_count = data.get('file_count', 0)
        infos.error = data.get('error', None)
        return infos

    def to_dict(self):
        return {
            'name': self.name,
            'status': self.status.name,
            'duration': self.duration,
            'disk_usage': self.disk_usage,
            'file_count': self.file_count,
            'error': self.error
        }
