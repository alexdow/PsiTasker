import MySQLdb
import time
import simplejson as json
import os

class BlogTask():
    STATUS_PENDING = "Pending"
    STATUS_STARTED = "Started"
    STATUS_FINISHED = "Finished"
   
    def __init__(self, db, logger, task_id=0):

        self.TYPE_PROCESS_MP3 = "mp3"
        self.TYPE_PROCESS_VIDEO = "video"
        self.TYPE_MISC = "misc"

        self.db = db
        self.task_id = task_id
        self.logger = logger
        self.task_pid = os.getpid()
        
        cursor = db.cursor()

        if self.task_id != 0:        
            results = cursor.execute("""SELECT * FROM sys_tasks WHERE id=%s""",(self.task_id))
            
            row = cursor.fetchall()[0]
            self.task_type = row["task_type"]
            self.task_status = row["task_status"]
            self.task_pid = row["task_pid"]
            self.added_on = row["added_on"]
            self.started_on = row["started_on"]
            self.finished_on = row["finished_on"]
            self.user_id = row["user_id"]
            
            if row["args"] != "" and row['args'] != None:
                self.logger.info(row["args"])
                self.args = json.loads(row["args"])
            else: 
                self.args = {}

        else:
            self.task_type = None
            self.task_status = None
            self.task_pid = None
            self.added_on = None
            self.started_on = None
            self.finished_on = None
            self.user_id = None
            self.args = {}

    def save(self):
        
        cursor = self.db.cursor()       
 
        if self.task_id > 0:

            args = json.dumps(self.args)

            query = """UPDATE sys_tasks SET task_type=%s, task_status=%s, task_pid=%s, added_on=%s, finished_on=%s, started_on=%s, user_id=%s, args=%s WHERE id=%s"""
            cursor.execute(query, (self.task_type, self.task_status, self.task_pid, self.added_on, self.finished_on, self.started_on, self.user_id, args, self.task_id))
            self.db.commit()
        else:
            self.added()
            args = json.dumps(self.args)
            query = """INSERT INTO sys_tasks (task_type,task_status,task_pid,added_on,finished_on,started_on,user_id,args) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
            cursor.execute(query,(self.task_type, self.task_status, self.task_pid, self.added_on, self.finished_on, self.started_on, self.user_id, args))
            cursor.close()
            self.db.commit()

            cursor = self.db.cursor()
            query = "SELECT LAST_INSERT_ID() AS last_id"
            cursor.execute(query)
            row = cursor.fetchall()[0]
            self.task_id = int(row[0])
            cursor.close()

    def addArg(self, key, value):
        self.args[key] = value

    def run(self):
        pass 

    def added(self):
        self.setStatus(BlogTask.STATUS_PENDING)
        self.added_on = time.strftime("%Y-%m-%d %H-%M-%S")

    def finished(self):
        self.setStatus(BlogTask.STATUS_FINISHED)
        self.finished_on = time.strftime("%Y-%m-%d %H-%M-%S")
        self.save()

    def start(self):
        self.started()
        self.run()
        self.finished()

    def started(self):
        self.started_on = time.strftime("%Y-%m-%d %H-%M-%S")
        self.setStatus(BlogTask.STATUS_STARTED)
        self.save()

    def setStatus(self, status):
        self.logger.info("[" + str(self.task_pid) + "] Status: " + status)
        self.task_status = status
   

