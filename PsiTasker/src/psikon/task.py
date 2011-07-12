'''
Created on 2011-07-10

@author: v0idnull
'''
import threading
import MySQLdb
import logging

class TaskNotFound(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class Factory(object):
    
    def __init__(self, config):
        self.config = config
    
    def getTaskObject(self, taskId):
        pass


class TaskObject(object):
    
    def __init__(self, config, db, logger, taskId):
        self.db = db
        self.logger = logger
        self.taskId = taskId
        self.config = config
        
        
        self.STATUS_PENDING = "pending"
        self.STATUS_WORKING = "working"
        self.STATUS_STARTING = "starting"
        self.STATUS_FINISHED = "finished"
        self.STATUS_ERROR = "error"
        
        cur = self.db.cursor()
        cur.execute("SELECT * FROM " + self.config["db"]["table_name"] + "WHERE id=%s", (self.taskId))
        
        if cur.rowcount == 0:
            raise TaskNotFound(self.taskId)
        
        row = cur.fetchone()
        self.status = self.STATUS_PENDING
        
    def setStatus(self, status):
        cur = self.db.cursor()
        cur.execute("UPDATE " + self.config["db"]["table_name"] + " SET status=%s WHERE id=%s", (status, self.taskId))
        self.logger.info("Task [" + self.taskId + "]: Status changed to '" + status + "'")
        self.status = status
        
        
        
        
    
    
class ThreadObject(threading.Thread):
    
    def __init__(self, config):
        
        self.config = config
        self.db = MySQLdb.connect(host=self.config["db"]["host"], user=self.config["db"]["user"], passwd=self.config["db"]["pass"], db=self.config["db"]["dbname"])
        
        self.logger = logging.getLogger("psitask")
        self.logger.setLevel(self.config["log"]["level"])
        
        formatter = self.logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
        fh = self.logging.FileHandler(self.config["log"]["path"])
        fh.setLevel(self.config["log"]["level"])
        fh.setFormatter(formatter)        
        
        threading.Thread.__init__(self)
        
    def run(self):
        
        logger.debug("Thread: " + self.getName() + " -- running")
        
        self.taskObj.pre()
        self.taskObj.start()
        self.taskObj.post()
        
        