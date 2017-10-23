import requests
import time
from config import conf

now = time.strftime("%Y-%m-%d %H:%M:%S")
print "%s start predispatch" % now
api = "http://%s/apo/timeout_check" % conf.server
resp = requests.get(api)
now = time.strftime("%Y-%m-%d %H:%M:%S")
print now ,resp.text

