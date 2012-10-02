#This file is to define the redis operations
#
# Only Provide the getconn service
# Project: Weardex.com
# Author: Zhang Weidong
# Class: RedisOperation
#
# Provide the following interface and inplement
#	1.__init__(self)
#	2.getconn(self)
#	3.set(self, key, value)
#   4.get(self)
#   5.sadd(self, userset, uservalue)
#	.....
###############################################
from  datetime import datetime, date, time
from settings import Settings
import redis
import logging

# Create logging handler
now = datetime.now()
nowstr = str(now.year) + str(now.month) + str(now.day) + str(now.hour)
logger = logging.getLogger('crawler')
hdlr = logging.FileHandler('crawler-redis-' + nowstr + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

class RedisOperation(object):
	# Get the settings from the setting file
	settings         = Settings()
	
	def __init__(self, pool):
		# Get the start datetime
		self.pool = pool #redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=int(self.redis_db))
		self.conn = redis.Redis(connection_pool = self.pool)
		self.logger = logger

	# Connection Factory: connection provider
	def getconn(self):
		return self.conn
	
	# Get redis urlhash db connection
	#def geturlhashconn(self):
		#return self.urlhash_conn
	
	# Operate the redis through the following funcs is more
	# Safty than self.conn.sadd(...)
	def set(self, key, value):
		try:
			self.conn.set(key, value)
		except Exception:
			self.logger.error("set error, key:" + str(key) + ",value:" + value)
	
	# Get the mapped valued of 'key' from redis
	def get(self, key):
		try:
			value = self.conn.get(key)
			if value != None:
				return value
			else:
				self.logger.warning("get None, key:%s"%str(key))
				return None
		except Exception:
			self.logger.error('save original image failed...error:%s'%(str(e)))
			return None
	
	# Add 'uservalue' to 'uesrset'
	def sadd(self, userset, uservalue):
		try:
			return self.conn.sadd(userset, uservalue)
		except Exception:
			self.logger.error("sadd error,set:" + userset + ",value:" + uservalue)
			return 0
	
	# Push one member from 'userlist's front end
	def spop(self, userset):
		try:
			value = self.conn.spop(userset)
			if value != None:
				return value
			else:
				self.logger.warning("spop None, set:" + userset)
				return None
		except Exception:
			self.logger.error("spop error, set:" + userset)
			return None

	# Push one member from 'userlist's tail end
	def lpush(self, userlist, uservalue):
		try:
			return self.conn.lpush(userlist, uservalue)
		except Exception:
			self.logger.error("lpush error,list:" + userlist + "value:" + uservalue)
			return 0

	# Push one member from 'userlist's tail end
	def check_lpush(self, userset, usersetvalue, userlist, userlistvalue):
		try:
			re = self.sadd(userset, usersetvalue)
			if re == 1:
				return self.conn.lpush(userlist, userlistvalue)
			else:
				return 0
		except Exception:
			self.logger.error("lpush error,list:" + userlist + "value:" + userlistvalue)
			return 0

	# Pop one member from 'userlist's tail end
	def lpop(self, userlist):
		try:
			value = self.conn.lpop(userlist)
			if value != None:
				return value
			else:
				self.logger.warning("lpop error, list:" + userlist)
				return None
		except Exception:
			self.logger.error("lpop error,list:" + userlist)
			return None

	# Pop one member from 'userlist's front end
	def rpop(self, userlist):
		try:
			value = self.conn.rpop(userlist)
			if value != None:
				return value
			else:
				self.logger.warning("rpop error, list:" + userlist)
				return None
		except Exception:
			self.logger.error("rpop error,list:" + userlist)
			return None
	
	# Pop one member from 'userset'
	def spop(self, userset):
		try:
			value = self.conn.spop(userset)
			if value != None:
				return value
			else:
				self.logger.warning("spop error, list:" + userset)
				return None
		except Exception:
			self.logger.error("spop error,list:" + userset)
			return None

	# Check 'value' is in 'userset', if in return 1, or return 0
	def sismember(self, userset, value):
		try:
			re = self.conn.sismember(userset, value)
			if re == True:
				return 1
			else:
				return 0
		except Exception:
			self.logger.error("sismember error, set:" + userset + ",url:" + value)
			return 0

	# Clear db
	def cleardb(self):
		try:
			# conn.flushdb(), if success return True, or return False
			re = self.conn.flushdb()
			if re:
				return 1
			else:
				return 0
		except Exception:
			self.logger.error("flushdb error")
			return 0

	# level-0 do nothing, level-1 delete the list not set, level-2 delete all elements includes list and set
	def deletedb(self, listname, page_url_set, del_level):
		try:
			if del_level == 0:
				pass
			elif del_level == 1:
				self.conn.delete(listname)
			elif del_level == 2:
				self.conn.delete(listname, page_url_set)
			elif del_level == 3:
				self.cleardb()
			else:
				pass
		except Exception:
			self.logger.error("delete with level error")
			return 0
