
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

def RefreshSubTask(db):
    try:
        cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SELECT * FROM apo_sub_task WHERE exec_time < NOW() AND status=1")
        if cur.rowcount <= 0:
            print "NO SUB TASK NEED DISPATCH"
        db.autocommit(False)
        for subTask in cur.fetchall():
            subTaskId = subTask['id']
            apoId = subTask['apo_id']
            count = subTask['count']
            if count <= 0:
                print "INVALID SUB TASK COUNT %d:%d" % (apoId, count)
                continue
            print "UPDATE APO[%d] COUNT[%d]" % (apoId, count)
            cur.execute("UPDATE apo_app_info set total=total+%d, redispatch=1 where id=%d" % (count, apoId))
            cur.execute("UPDATE apo_sub_task set status=0 where id=%d" % subTaskId)
        db.commit()
    except Exception, e:
        print "Refresh sub task ok"
        traceback.print_exc()

def main():
    db = ConnectMysql(conf.mysql.host, conf.mysql.user, conf.mysql.passwd, conf.mysql.db, conf.mysql.port)
    if not db:
        print "CONNECT MYSQL DB ERROR"
        return 0
    RefreshSubTask(db)

if __name__ == "__main__":
    main()
