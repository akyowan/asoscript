#!/usr/bin/python
#coding:utf-8
import redis
import MySQLdb
import json
import time
import sys
import traceback
import datetime
from config import conf


STATS_LIST = [
        "SPEED_STATS_SUCCESS",
        "SPEED_STATS_REQUEST",
        "SPEED_STATS_TIMEOUT",
        "SPEED_STATS_RECYLE",
        "SPEED_STATS_FAILED",
        ]

ERR_LIST = {
    "EMDOWNRET_APPSTORE_SHOWERROR"      :2,
    "EMDOWNRET_SEARCHPAGE_SHOWERROR"    :3,
    "EMDOWNRET_NOTFINDAPP"              :4, 
    "EMDOWNRET_ACCOUNT_NEED_VERIFY"     :5,
    "EMDOWNRET_VERIFY_FAIL"             :6,
    "EMDOWNRET_BUY_FAIL"                :7,
    "EMDOWNRET_DOWNLOAD_FAIL"           :8,
    "EMDOWNRET_DOWNLOAD_FAIL2"          :9,
    "EMDOWNRET_SERVER_ERROR"            :10,
    "EMDOWNRET_UNKNOW_ALERT"            :11,
    "EMDOWNRET_APPSTORE_DIE"            :12,
    "EMDOWNRET_ACCOUNT_DISABLE"         :13,
    "EMDOWNRET_NOT_IUPUT_PW"            :14,
    "EMDOWNRET_FAIL"                    :15,
    "EMDOWNRET_BUY_DETECT_FAIL"         :16,
    "EMDOWNRET_ACCOUNT_NEED_CHECK"      :17,
    "EMDOWNRET_APPSTORE_DIE_DETAIL"     :18,
    "EMDOWNRET_APPSTORE_DIE_OPEN_AP"    :19,
    "EMDOWNRET_TOUCHSPRITE_DIE"         :20
}

#       ERR_LIST = {
#       "EMDOWNRET_APPSTORE_SHOWERRO"    : 2,
#       "EMDOWNRET_SEARCHPAGE_SHOWERROR" : 3,
#       "EMDOWNRET_NOTFINDAPP"           : 4,
#       "EMDOWNRET_ACCOUNT_NEED_VERIF"   : 5,
#       "EMDOWNRET_VERIFY_FAI"           : 6,
#       "EMDOWNRET_BUY_FAI"              : 7,
#       "EMDOWNRET_DOWNLOAD_FAI"         : 8,
#       "EMDOWNRET_DOWNLOAD_FAIL"        : 9,
#       "EMDOWNRET_SERVER_ERRO"          : 10,
#       "EMDOWNRET_UNKNOW_ALER"          : 11,
#       "EMDOWNRET_APPSTORE_DI"          : 12,
#       "EMDOWNRET_ACCOUNT_DISABL"       : 13,
#       "EMDOWNRET_NOT_IUPUT_P"          : 14,
#       "EMDOWNRET_FAI"                  : 15,
#       "EMDOWNRET_BUY_DETECT_FAI"       : 16,
#       "EMDOWNRET_ACCOUNT_NEED_CHEC"    : 17
#       }

GRAN_SIZE= 10*60


ALIVE_KEY = "DEVICE_ALIVE_LIST"
ALIVE_TIME = 3*60

STATS_INFO = "REAL_STATS_INFO"
STATS_ERR = "REAL_STATS_ERR"

STATS_DB = 10

def ConnectRedis(db):
    try:
        r = redis.Redis(conf.redis.host, port=conf.redis.port, db=STATS_DB)
        if r:
            return r
        return True
    except Exception, e:
        print "    CONNECT REDIS FAILED:%r" % e
        traceback.print_exc()
        return False

def CmpStatsResult(db, key, granSize):
    try:
        start = int(time.time()) - granSize
        db.zremrangebyscore(key, 0, start)
        count = db.zcard(key)
        db.hset(STATS_INFO, key, count)
        return count
    except Exception, e:
        print "CMP STATS[%s] ERROR:%r" % (key, e)
        traceback.print_exc()

def CmpAliveDevice(db, key, aliveTime):
    try:
        start = int(time.time()) - aliveTime
        db.zremrangebyscore(key, 0, start)
        count = db.zcard(key)
        db.hset(STATS_INFO, key, count)
        return count
    except Exception, e:
        print "CMP STATS[%s] ERROR:%r" % (key, e)
        traceback.print_exc()

def CmpErrStatsResult(db, granSize):
    global ERR_LIST
    global STATS_ERR
    try:
        for k in ERR_LIST:
            key = "ERR_STATS_%d" % ERR_LIST[k]
            start = int(time.time()) - granSize
            db.zremrangebyscore(key, 0, start)
            count = db.zcard(key)
            db.hset(STATS_ERR, k, count)
        return True
    except Exception, e:
        print "CMP STATS[%s] ERROR:%r" % (key, e)
        traceback.print_exc()

def main():
    db = ConnectRedis(STATS_DB)
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not db:
        print "CONNECT REDIS[%d] FAILED" % STATS_DB
    for key in STATS_LIST:
        count = CmpStatsResult(db, key, GRAN_SIZE)
        print "%s %s %d" % (now, key, count)
    CmpAliveDevice(db, ALIVE_KEY, ALIVE_TIME)
    CmpErrStatsResult(db, GRAN_SIZE)
    print "%s %d" % (ALIVE_KEY, count)

if __name__ == "__main__":
    main()
