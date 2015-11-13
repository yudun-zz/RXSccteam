#! /bin/bash

import MySQLdb
import _mysql_exceptions
import os

datapath = "/home/ubuntu/ccteam/backend/db/q4_finaldata"
sql = """load data infile '%s' into table q4_table FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';"""

db = MySQLdb.connect(host="localhost",user = "root", passwd="1314159", db="twitter", charset='utf8', use_unicode=True)
cursor = db.cursor()

for i in os.listdir(datapath):
	loadsql = sql % i
	cursor.execute(loadsql)
	db.commit()
	print "Finish Handling " + i + "..."
