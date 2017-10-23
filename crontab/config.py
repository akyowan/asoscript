import json

class Redis:
    def __init__(self, conf):
        self.host = conf["host"]
        self.port= conf["port"]
        self.db = conf["db"]

class Mysql:
    def __init__(self, conf):
        self.host = conf["host"]
        self.port= conf["port"]
        self.db= conf["db"]
        self.user = conf["user"]
        self.passwd = conf["passwd"]
        
class Cache:
    def __init__(self, conf):
        self.min = conf["min"]
        self.max = conf["max"]

class Config:
    def __init__(self):
        f = open("/data/config/crontab.json")
        conf = json.load(f)
        f.close()
        self.redis = Redis(conf["redis"])
        self.mysql = Mysql(conf["mysql"])
        self.cache = Cache(conf["cache"])
        self.server = conf["server"]

conf = Config()

