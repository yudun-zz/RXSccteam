#! /bin/bash

import MySQLdb
import json
import _mysql_exceptions
import os

datapath = '/home/osboxes/Developer/ccteamproj/phase1/testdata'
sql = """INSERT INTO tweetscore VALUES (%s, %s, %s, %s, %s)"""

db = MySQLdb.connect(host="localhost",user = "root", passwd="Sxt19920524", db="twitter", charset='utf8', use_unicode=True)
cursor = db.cursor()
#cursor.execute("SET NAMES utf8")

for i in os.listdir(datapath):
	path = datapath + '/' + i
	data_file = open(path)
	for line in data_file.readlines():
		try:
			#line = unicode(line,errors='ignore')
			tweet = json.loads(line)
			#Filter zero length user id and tweet id
			if len(tweet['tid']) == 0 or len(tweet['uid']) == 0:
				continue
			cursor.execute(sql, (tweet['tid'], tweet['score'], tweet['uid'], tweet['text'], tweet['t']))
		except MySQLdb.IntegrityError,e:
			#in case of duplicate tweets, we ignore the later ones
			continue
		except ValueError,e:
			continue
	db.commit()