import json


class objectview(object):
    def __init__(self, d):
        self.__dict__ = d

    def __repr__(self):
        return (
            self.__class__.__name__
            + "("
            + ", ".join([f"{k}={v}" for k, v in self.__dict__.items()])
            + ")"
        )

    def update(self, d):
        self.__dict__.update(**d)

    def to_json(self):
        return json.dumps(self.__dict__)


class Config(objectview):
    api_key = None
    databricks_url = None
    cluster_id = None
