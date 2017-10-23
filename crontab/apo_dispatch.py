#!/usr/bin/python
#coding:utf-8
import redis
import MySQLdb
import json
import time
import sys
import traceback
import datetime
import random
from config import conf

MAX_LEN = conf.cache.max
MIN_LEN = conf.cache.max


def ConnectRedis():
    try:
        r = redis.Redis(host=conf.redis.host, port=conf.redis.port, db=conf.redis.db)
        if r:
            return r
        return True
    except Exception, e:
        print "    CONNECT REDIS FAILED:%r" % e
        traceback.print_exc()
        return False

def ConnectMysql(host, user, passwd, db, port):
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, port=int(port))
        conn.set_character_set('utf8')
        cur = conn.cursor()
        cur.execute('SET NAMES utf8;') 
        cur.execute('SET CHARACTER SET utf8;')
        cur.execute('SET character_set_connection=utf8;')
        return conn
    except Exception, e:
        print "Connect %s %s %s %s %s error[%r]" % (host, port, user, passwd, db, e)
        traceback.print_exc()
        return False;


def GetEnableListLen(redis):
    try:
        return redis.llen("APO_ENABLE_LIST")
    except Exception, e:
        print "GET APO_ENABLE_LIST LENGTH FAILED"
        traceback.print_exc()

def AddEnableApo(redis, apo):
    try:
        return redis.rpush("APO_ENABLE_LIST", json.dumps(apo))
    except Exception, e:
        print "ADD APO TO APO_ENABLE_LIST LENGTH"
        traceback.print_exc()

def GetEnaleApps(db):
    try:
        cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SELECT app_id FROM apo_available GROUP BY app_id")
        if cur.rowcount <= 0:
            print "GET APP ID FROM DB FAILED"
            return False
        appIDs = []
        for ins in cur.fetchall():
            appIDs.append(ins["app_id"])
            return appIDs
    except Exception, e:
        print "GET APP ID FROM DB FAILED"
        traceback.print_exc()

def GetEnaleApos(db):
    try:
        cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SELECT apo_available.apo_id ,apo_app_info.app_id FROM apo_available ,apo_app_info WHERE apo_available.apo_id = apo_app_info.id GROUP BY apo_id ORDER BY apo_app_info.update_time")
        if cur.rowcount <= 0:
            print "GET APP ID FROM DB FAILED"
            return False
        appIDs = []
        for ins in cur.fetchall():
            appIDs.append({"apo_id":ins["apo_id"], "app_id":ins["app_id"]})
        return appIDs
    except Exception, e:
        print "GET APP ID FROM DB FAILED"
        traceback.print_exc()

def DispatchEnableAposV3(db, redis, need):
    try:
        lastApo = GetLastDispatchApp(redis)
        apos = GetEnaleApos(db)
        if not apos:
            print "NO APPS NEED DISPATCH"
            return 0
        if not lastApo:
            newApo = apos[0]
            count = DispatchTasks(db, redis, newApo["apo_id"], need)
            SetLastDispatchApo(redis, newApo)
            return count
        count = DispatchTasks(db, redis, lastApo["apo_id"], need)
        if (need - count) > 0:
            newApo = None
            for apo in apos:
                if apo["app_id"] != lastApo["app_id"]:
                    newApo = apo
                    break
            if newApo == None:
                newApo = apos[0]
            SetLastDispatchApo(redis, newApo)
        return count 
    except Exception, e:
        traceback.print_exc()
        return False

def DispatchTasks(db, redis, apo_id, count):
    try:
        cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SELECT * FROM apo_available WHERE apo_id='%s' ORDER BY update_time ASC, level DESC, id ASC LIMIT %d" % (apo_id, count))
        if cur.rowcount <= 0: 
            print "DISPATCH APO:%s NNED:%d FAILED" % (apo_id, count)
            return 0
        apos = []
        db.autocommit(False)
        for apo in cur.fetchall():
            apo["dispatch_time"] = str(apo["dispatch_time"])
            apo["start_time"] = str(apo["start_time"])
            apo["update_time"] = str(apo["update_time"])
            cur.execute("DELETE FROM apo_available WHERE id=%d" % apo["id"])
            apos.append(apo)
        db.commit()
        for apo in apos:
            AddEnableApo(redis, apo)
        print "DISPATCH APO:%s COUNT:%d" % (apo_id, len(apos))
        cur.close()
        return len(apos)
    except Exception, e:
        print "GET APO_ENABLE_LIST LENGTH FAILED"
        traceback.print_exc()

def DispatchEnableAposV2(db, redis, apps, count):
    try:
        cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        tasks = []
        db.autocommit(False)
        for app_id in apps:
            cur.execute("SELECT * FROM apo_available WHERE app_id='%s' ORDER BY id ASC LIMIT %d" % (app_id, count))
            if cur.rowcount <= 0:  
                print "GET AVAIABLE APP[%s] FAILED" % app_id
                continue
            print "DISPATCH APP[%s] %d" % (app_id, cur.rowcount)
            for apo in cur.fetchall():
                apo["dispatch_time"] = str(apo["dispatch_time"])
                apo["start_time"] = str(apo["start_time"])
                apo["update_time"] = str(apo["update_time"])
                cur.execute("DELETE FROM apo_available WHERE id=%d" % apo["id"])
                tasks.append(apo)
                db.commit()
        random.shuffle(tasks)
        for t in tasks:
            AddEnableApo(redis, t)
        return len(tasks)
    except Exception, e:
        print "GET APO_ENABLE_LIST LENGTH FAILED"
        traceback.print_exc()
        return 0

def DispatchEnableApos(db, redis, count):
    try:
        cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SELECT * FROM apo_available ORDER BY update_time ASC, level DESC, id ASC LIMIT %d" % count)
        if cur.rowcount <= 0: 
            print "GET AVAIABLE APO FAILED"
            return 0
        apos = []
        db.autocommit(False)
        for apo in cur.fetchall():
            apo["dispatch_time"] = str(apo["dispatch_time"])
            apo["start_time"] = str(apo["start_time"])
            apo["update_time"] = str(apo["update_time"])
            cur.execute("DELETE FROM apo_available WHERE id=%d" % apo["id"])
            apos.append(apo)
        db.commit()
        random.shuffle(apos)
        for apo in apos:
            AddEnableApo(redis, apo)
        print "DISPATCH APO %d" % count
        cur.close()
    except Exception, e:
        print "GET APO_ENABLE_LIST LENGTH FAILED"
        traceback.print_exc()

def UpdateApoStatus(db):
    try:
        cur = db.cursor()
        cur.execute("UPDATE apo_app_info SET redispatch=1 WHERE total > dispatch_count AND status=1")
        print "UPDATE %d APO STATUS" % cur.rowcount
        db.commit()
        cur.close()
    except Exception, e:
        print "UPDATE APO STATUS FAILED"
        traceback.print_exc()

def GetLastDispatchApp(redis):
    try:
        app = redis.get("APO_DISPATCH_LAST_APP")
        if not app:
            return None
        return json.loads(app)
    except Exception, e:
        traceback.print_exc()
        return None

def SetLastDispatchApo(redis, app):
    try:
        redis.set("APO_DISPATCH_LAST_APP", json.dumps(app))
        return True
    except Exception, e:
        traceback.print_exc()
        return None

def main():
    global MAX_LEN
    db = ConnectMysql(conf.mysql.host, conf.mysql.user, conf.mysql.passwd, conf.mysql.db, conf.mysql.port)
    if not db:
        print "CONNECT MYSQL DB ERROR"
        return 0
    redis = ConnectRedis()
    if not redis:
        print "CONNECT REDIS ERROR"
        return 0
    UpdateApoStatus(db)
    count = GetEnableListLen(redis)
    if count > MAX_LEN:
        print "NO NEED DISPATCH APO NOW[%d]" % count
        return 0

    need = (MAX_LEN - count)
    if need < MIN_LEN:
        print "NO NEED DISPATCH APO NEED[%d]" % need 
        return 0
    while(need !=0):
        count = DispatchEnableAposV3(db, redis, need)
        if count == 0:
            break
        need -= count
if __name__ == "__main__":
    main()
