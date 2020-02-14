import uuid


class Comm(object):
    on_recv = None

    def __init__(self, target, on_recv):
        self.uuid = uuid.uuid4().hex
        self.target = target
        self.on_recv = on_recv

    def __str__(self):
        return self.target
