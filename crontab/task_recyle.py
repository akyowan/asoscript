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

MIN_RG = 1
RS = 0
RE = 1
ACCOUNT_DISABLE_DAY = 4


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
        print "    Connect %s %s %s %s %s error[%r]" % (host, port, user, passwd, db, e)
        traceback.print_exc()
        return False;

def SetPreDispatchStatus(redis, status):
    try:
        return redis.set("IS_ENABLE_DISPATCH", status)
    except Exception, e:
        print "    SET DISABLE DISPATCH FAILED:%r" % e
        traceback.print_exc()
        return False

def StartApoRecyle(redis):
    try:
        delay = 1
        count = 0
        while (True):
            status = redis.get("DISPATCH_STATUS")
            if status == "START":
                count += 1
                print "DISPATCH HAD START DELAY %d" % count 
                time.sleep(delay)
                continue
            else:
                break
            if count > 1800:
                print "WAIT DISPATCH STOP FAILED %d" % count
                break
        return redis.set("DISPATCH_STATUS", "START")
    except Exception, e:
        print "    SET DISABLE DISPATCH FAILED:%r" % e
        traceback.print_exc()
        return False

def StopApoRecyle(redis):
    try:
        status = redis.get("DISPATCH_STATUS")
        if status == "STOP":
            print "INVALID DISPATCH STATUS, STOP BY SOMEONE"
        return redis.set("DISPATCH_STATUS", "STOP")
    except Exception, e:
        print "    SET DISABLE DISPATCH FAILED:%r" % e
        traceback.print_exc()
        return False

def SwitchCompleteTable(redis, tableName):
    try:
        return redis.set("APO_COMPLETE_TABLE", tableName)
    except Exception, e:
        print "    SWITCH APO COMPLETE TABLE FAILED:%r" % e
        traceback.print_exc()
        return False

def IsTableExist(tableName, conn) :
    try:
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        for table in cur.fetchall():
            if table[0] == tableName:
                return True
        return False 
    except Exception, e:
        print "    CHECK TABLE[%s] IS EXIST FAILED:%r" % (tableName, e)
        traceback.print_exc()
        return False

def CreateNewApoCompleteTable(tableName, conn):
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE `%s` ( \
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT, \
            `apo_id` int(11) unsigned NOT NULL DEFAULT '0', \
            `account` varchar(64) COLLATE utf8_bin NOT NULL, \
            `account_id` int(11) NOT NULL, \
            `account_brief` varchar(64) COLLATE utf8_bin NOT NULL, \
            `sn` varchar(64) COLLATE utf8_bin NOT NULL, \
            `app_id` varchar(64) COLLATE utf8_bin NOT NULL, \
            `apo_key` varchar(255) COLLATE utf8_bin NOT NULL, \
            `level` int(11) unsigned NOT NULL DEFAULT '0', \
            `dispatch_time` datetime NOT NULL, \
            `start_time` datetime DEFAULT NULL, \
            `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP, \
            `ip` varchar(16) COLLATE utf8_bin DEFAULT NULL, \
            `apo_ip` varchar(255) COLLATE utf8_bin DEFAULT NULL, \
            `device_id` varchar(64) COLLATE utf8_bin DEFAULT NULL, \
            `round` int(11) NOT NULL DEFAULT '0', \
            `errno` int(11) DEFAULT NULL, \
            `status` int(11) NOT NULL DEFAULT '0', \
            PRIMARY KEY (`id`), \
            KEY `account_id` (`account_id`,`app_id`) USING BTREE, \
            KEY `account_brief` (`account_brief`) USING BTREE \
            ) ENGINE=InnoDB AUTO_INCREMENT=106868 DEFAULT CHARSET=utf8 COLLATE=utf8_bin" % tableName)
        conn.commit()
        return True
    except Exception, e:
        print "    CREATE COMPLETE TABLE FAILED:%r" % e
        traceback.print_exc()
        return False

def RecyleAllAccount(conn):
    try:
        cur = conn.cursor()
        cur.execute('UPDATE apo_account_info SET used_today=0 WHERE used_today>=1 AND \
                TIMESTAMPDIFF(DAY,DATE_FORMAT(update_time,"%Y-%m-%d"),DATE_FORMAT(NOW(),"%Y-%m-%d")) >=' + "%d" % (ACCOUNT_DISABLE_DAY));
        count = cur.rowcount
        cur.close()
        conn.commit()
        return count
    except Exception, e:
        print "    RECYLE ACCOUNT FAILED:%r" % e
        traceback.print_exc()
        return False

def SetUsedAccount(conn, ranges):
    try:
        cur = conn.cursor()
        count = 0
        db.autocommit(False)
        for r in ranges:
            rs = int(r[RS])
            re = int(r[RE])
            cur.execute("UPDATE apo_account_info SET used_today=1 WHERE id>=%d AND id<=%d" % (rs, re))
            count += cur.rowcount
            print "    RANGE[%d %d] COUNT[%d]" % (rs, re, cur.rowcount)
        conn.commit()
        return count
        cur.close()
    except Exception, e:
        print "    SET ACCOUNT USED FAILED:%r" % e
        traceback.print_exc()
        return False

def ResetUsedCache(redis, ranges):
    try:
        redis.delete("TODAY_USED_RANGE")
        for (k,v) in ranges.items():
            if not v:
                continue
            s = json.dumps(v)
            redis.hset("TODAY_USED_RANGE", k, s)
        return True
    except Exception,e:
        print "    RESET USED CACHE FAILED:%r" % e
        traceback.print_exc()
        return False

def GetApoAvaliable(conn, redis, briefs):
    try:
        accountMap = {}
        cur = conn.cursor()
        for brief in briefs:
            accountMap[brief] = []
            cur.execute("SELECT account_id FROM apo_available WHERE account_brief='%s' ORDER BY account_id ASC" % brief)
            if cur.rowcount <= 0:
                continue
            for account in cur.fetchall():
                accountMap[brief].append(int(account[0]))
            print "     AVAILABLE %s|%d" % (brief, len(accountMap[brief]))
        return accountMap
    except Exception, e:
        print "     GET AVALIABLE APO FAILED:%r" % e
        traceback.print_exc()
        return False
    
def GetCompleteAccount(conn, redis, accountMap):
    try:
        tableName = redis.get("APO_COMPLETE_TABLE")
        if not tableName:
            return False
        cur = conn.cursor()
        res = cur.execute("SELECT account_brief, account_id FROM %s" % tableName)
        for ins in cur.fetchall():
            brief = ins[0]
            if not accountMap[brief]:
                accountMap[brief] = []
            accountId = ins[1]
            accountMap[brief].append(int(accountId))
            print "    USED ACCOUNT %s|%d" % (brief, len(accountMap[brief]))
        return accountMap
    except Exception, e:
        print "    GET AVALIABLE APO FAILED:%r" % e
        traceback.print_exc()

def CmpUsedRange(accounts):
    try:
        if not accounts:
            return False
        accounts.sort()
        ranges = []
        end = accounts[0]
        begin = accounts[0]
        end = accounts[0]
        for cur in accounts:
            inter = int(cur) - int(end)
            if (inter > MIN_RG):
                curRange = [begin, end]
                begin = cur
                ranges.append(curRange)
            end = cur
        ranges.append([begin, cur])
        return ranges
    except Exception, e:
        print "    CMPUTER ACCOUNT USED RANGE FAILED:%r" % e
        traceback.print_exc()

def GetCacheAccounts(redis, accountMap):
    # GET ENABLE CACHE 
    try:
        enableList = redis.lrange('APO_ENABLE_LIST', 0, -1)
        for ins in enableList:
            apoInfo = json.loads(ins)
            if not apoInfo:
                continue
            brief = apoInfo['account_brief']
            accountID = apoInfo['account_id']
            if accountMap[brief]:
                accountMap[brief].append(accountID)
            else:
                accountMap[brief] = []
                accountMap[brief].append(accountID)
        doingList = redis.hgetall('APO_DOING_LIST')
        for key in doingList:
            apoInfo = json.loads(doingList[key])
            if not apoInfo:
                continue
            brief = apoInfo['account_brief']
            accountID = apoInfo['account_id']
            if accountMap[brief]:
                accountMap[brief].append(accountID)
            else:
                accountMap[brief] = []
                accountMap[brief].append(accountID)
        return accountMap
    except Exception, e:
        print "    ATTACH CACHE ACCOUNTS FAILED:%r" % e
        traceback.print_exc()
        return False

def GetAllBriefs(db, redis):
    try:
        cur = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SELECT brief FROM apo_account_groups")
        if cur.rowcount < 0:
            print "GET ACCOUNT BRIEF FAILED"
            return False
        briefs = []
        for ins in cur.fetchall():
            briefs.append(ins['brief'])
        return briefs
    except Exception, e:
        print "    GET BRIEFS FAILED:%r" % e
        traceback.print_exc()
        return False

date = datetime.datetime.now().strftime("%Y%m%d")
print "START %s ACCOUNT INIT" % date
print "==========================================================================================="

print "CONNECT REDIS"
redis = ConnectRedis()
if not redis:
    print "CONNECT REDIS ERROR"
    exit(0)
print "CONNECT REDIS OK"
print "-----------------------"

print "CONNECT MYSQL DB"


db = ConnectMysql(conf.mysql.host, conf.mysql.user, conf.mysql.passwd, conf.mysql.db, conf.mysql.port)
if not db:
    print "CONNECT MYSQL DB ERROR"
    exit(0)
print "CONNECT MYSQL DB OK"
print "-----------------------"

# 开始APO回收
print "START APO RECYLE"
if not StartApoRecyle(redis):
    print "START APO RECYLE FAILED"
    exit(0)
print "START APO RECYLE OK"
print "-----------------------"

#if not SetPreDispatchStatus(redis, "FALSE"):
#    print "DISABLE PRE DISPATCH ERROR"
#    exit(0)
#print "DISABLE PRE DISPATCH OK"
#print "-----------------------"

# 创建新的任务完成记录表
tableName = "complete_%s" % date
print "CREATE NEW COMPETE TABLE[%s]" % tableName
if not IsTableExist(tableName, db):
    if not CreateNewApoCompleteTable(tableName, db):
        print "CREATE NEW COMPETE TABLE ERROR"
        StopApoRecyle(redis)
#       SetPreDispatchStatus(redis, "TRUE")
        exit(0)
    else:
        print "CREATE NEW COMPETE TABLE OK"
else:
    print "CREATE NEW COMPETE TABLE ERROR TABLE EXIST"
print "-----------------------"

# 切换到新的任务完成记录表
print "SWITCH NEW COMPLETE TABLE"
if not SwitchCompleteTable(redis, tableName):
    print "SWITCH NEW COMPLETE TABLE ERROR"
    exit(0)
print "SWITCH NEW COMPLETE TABLE OK"
print "-----------------------"

# 回收所有已用账号
print "RYCYLE ACCOUNT"
count = RecyleAllAccount(db)
if not count:
    print "RECYLE ACCOUNT NONE"
print "RECYLE ACCOUNT COUNT[%d]" % count
print "-----------------------"

# 获取apo_available表中所有任务记录
# 获取刚刚创建的任务记录表种所有任务记录(可能再这段时间有任务完成或者失败)
print "GET %s USED ACCOUNT" % date
print "-----------------------"
print "    GET AVAILABLE ACCOUTS"
print "    ------------"
briefs = GetAllBriefs(db, redis)
if not briefs:
    print "GET BRIEFS FAILED"
    briefs = []
accounts = GetApoAvaliable(db, redis, briefs)
if not accounts:
    print "GET AVAILABLE ACCOUTS NONE"
print "    -----------------------"
print "    MERGE COMPLETE ACCOUNT"
usedRanges = {}
accounts = GetCompleteAccount(db, redis, accounts)
if not accounts:
    print "GET AVAILABLE ACCOUTS NONE"

accounts = GetCacheAccounts(redis, accounts)
if not accounts:
    print "ATTACH CACHE ACCOUNTS ERROR"
else:
    # 计算当天初始已用账号区间
    for k in accounts.keys():
        if not accounts[k]:
            continue
        print "INIT TODAY USED    [%s] %d" % (k, len(accounts[k]))
        usedRanges[k] = CmpUsedRange(accounts[k])
    print "GET %s USED ACCOUNT OK" % date
    print "-----------------------"
    print "INIT %s ACCOUNT USED RANGE" % date
    for k in usedRanges:
        print "    SET USED ACCOUNT [%s]" % (k)
        # 初始化当天已用账号区间
        count = SetUsedAccount(db, usedRanges[k])
        if not count:
            print "    COUNT[NONE]"
        else:
            print "    INIT USED TODAY %s|%d" % (k, count)
        print "    -----------------------"
    print "INIT %s ACCOUNT USED RANGE OK" % date
    print "-----------------------"


# 重置当日账号使用区间缓存
print "RESET USED ACCOUNT CACHE"
if not ResetUsedCache(redis, usedRanges):
    print "RESET USED ACCOUNT CACHE FAILED"
else:
    print "RESET USED ACCOUNT CACHE OK"
print "-----------------------"

# 结束任务回收
print "STOP APO RECYLE"
if not StopApoRecyle(redis):
    print "STOP APO RECYLE"
print "STOP APO RECYLE OK"
print "-----------------------"
print "END %s ACCOUNT INIT" % date


