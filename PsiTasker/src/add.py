#!/usr/bin/env python

from blogtask import BlogTask
import logging
import MySQLdb
import sys

db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="psikon_blog")

logger = logging.getLogger("blog-task")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
fh = logging.FileHandler("blog-task.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

logger.addHandler(fh)

fn = sys.argv[1]
dbid = sys.argv[2]
userid = sys.argv[3]

task = BlogTask(db,logger)
task.addArg("file", fn)
task.addArg("dbid",dbid)
task.addArg("userid",userid)
task.task_type = task.TYPE_PROCESS_MP3
task.save()

