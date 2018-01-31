import json


class DefaultJSONEncoder(json.JSONEncoder):
    """
    Subclass of `json.JSONEncoder` that can serialize user-defined types.
    """
    def default(self, o):
        return o.__dict__
