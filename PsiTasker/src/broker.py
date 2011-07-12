#!/usr/bin/env python

from daemon import Daemon
import logging
import time
import sys
from blogtask import BlogTask
import MySQLdb
import threading
import subprocess
import os
from psikonoptions import PsikonOptions

logger = logging.getLogger("blog-task")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
fh = logging.FileHandler(PsikonOptions.LOG_FILE)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

logger.addHandler(fh)

class AudioTask(BlogTask):
    
    def __init__(self,db,logger,id=0):
        self.STATUS_COPYING = "Copying"
        self.STATUS_CONVERTING = "Converting"
        self.STATUS_GENIMG = "Generating Image"
        self.STATUS_CLEANUP = "Cleaning up"
        # super(AudioTask,self).__init__(db,logger,id)
        BlogTask.__init__(self, db, logger, id)
        self.task_pid = os.getpid()

    def run(self):
        ####
        ## Convert MP3 to WAV file with sox
        ####
        self.setStatus(self.STATUS_CONVERTING)
        soxreturn = subprocess.call(["sox", self.args["file"], self.args["file"].replace(".mp3",".wav")])
        self.logger.info("[" + str(self.task_pid) + "] sox return: " + str(soxreturn))

        ####
        ## Generate spectrograph and waveform images with wav2png.py
        ###
        self.setStatus(self.STATUS_GENIMG)
        wavreturn = subprocess.call([PsikonOptions.WAV2PNG, self.args["file"].replace(".mp3",".wav")])
        self.logger.info("[" + str(self.task_pid) + "] wav2png return: " + str(wavreturn))

        ####
        ## Remove WAV file that's no longer needed
        ####
        self.setStatus(self.STATUS_CLEANUP)
        rmreturn = subprocess.call(["rm","-f", self.args["file"].replace(".mp3",".wav")])
        self.logger.info("[" + str(self.task_pid) + "] rm return: " + str(rmreturn))

        ####
        ## Change the active flag of the mp3 we just finished processing
        ####
        self.logger.info("[" + str(self.task_pid) + "] Connecting to blog database...")
        db = MySQLdb.connect(host=PsikonOptions.BLOG_DB_HOST, user=PsikonOptions.BLOG_DB_USER, passwd=PsikonOptions.BLOG_DB_PASS, db=PsikonOptions.BLOG_DB_NAME)
        cur = db.cursor()
        blogid = self.args["dbid"]
        query = """UPDATE """ + PsikonOptions.BLOG_DB_MP3_TABLE + """ SET active=1 WHERE mp3_id=%s"""
        cur.execute(query,(blogid))
        cur.close()
        db.commit()
        db.close()

class TaskThread(threading.Thread):

    def __init__(self, task_id):
        self.task_id = task_id
        threading.Thread.__init__(self)

    def run(self):
        db = MySQLdb.connect(host=PsikonOptions.TASK_DB_HOST, user=PsikonOptions.TASK_DB_USER, passwd=PsikonOptions.TASK_DB_PASS, db=PsikonOptions.TASK_DB_NAME, cursorclass=MySQLdb.cursors.DictCursor)
        logger.info("Starting task #" + str(self.task_id)) 
        task = AudioTask(db, logger, self.task_id)
        task.start()


class Broker(Daemon):
    def run(selif):

        db = MySQLdb.connect(host=PsikonOptions.TASK_DB_HOST, user=PsikonOptions.TASK_DB_USER, passwd=PsikonOptions.TASK_DB_PASS, db=PsikonOptions.TASK_DB_NAME)
        counter = 0

        while True:

            counter = counter + 1
           
            totalThreads = threading.active_count() - 1 
            if totalThreads < PsikonOptions.THREAD_LIMIT:

                realLimit = PsikonOptions.THREAD_LIMIT - totalThreads

                cur = db.cursor()
                cur.execute("""SELECT id FROM sys_tasks WHERE task_status=%s LIMIT %s""", (BlogTask.STATUS_PENDING, realLimit))
                
                while True:
                    row = cur.fetchone()
                    if row == False or row == None:
                        break
                    logger.info("New task ID: " + str(row[0]))
                    TaskThread(row[0]).start()
                    
            
            if counter == 10:
                logger.info("Threads: " + str(threading.active_count()))
                counter = 0

            time.sleep(1)


b = Broker(PsikonOptions.PID_FILE, stdin=PsikonOptions.STDIN_FILE,
                         stdout=PsikonOptions.STDOUT_FILE,
                         stderr=PsikonOptions.STDERR_FILE)

        

if __name__== "__main__":

    if len(sys.argv) > 1:
        if sys.argv[1] == "start":
            b.start()
            print "Started"
        elif sys.argv[1] == "stop":
            print "Stopping... ",
            b.stop()
            print "done."
        else:
            print "Usage: broker.py [start|stop]"

