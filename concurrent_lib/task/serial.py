from concurrent_lib.task.group import Group


class Serial(Group):
    def split(self):
        first = self.tasks[0]
        remaining = Serial.from_iter(self.tasks[1:])
        remaining.tid = self.tid
        return first, remaining
