#! /bin/bash

import MySQLdb
import json
import _mysql_exceptions
import os
import codecs

#Json Format : raw, uid, cen, t, hash, tid, impact
'''
datapath = '/home/ubuntu/ccteam/backend/db/q3_testdata'
sql_pos = """INSERT INTO q3_positive VALUES (%s, %s, %s, %s, %s)"""
sql_neg = """INSERT INTO q3_negative VALUES (%s, %s, %s, %s, %s)"""

db = MySQLdb.connect(host="localhost",user = "root", passwd="1314159", db="twitter", charset="utf8mb4")
cursor = db.cursor()
cursor.execute("SET NAMES utf8mb4")
for i in os.listdir(datapath):
	path = datapath + '/' + i
	data_file = open(path)
	for line in data_file.readlines():
		try:
			tweet = json.loads(line)
			if tweet['impact'] == 0:
				continue
			else:
				if tweet['impact'] > 0:
					cursor.execute(sql_pos, (tweet['tid'], tweet['uid'], tweet['cen'], tweet['t'], tweet['impact']))
				else:
					cursor.execute(sql_neg, (tweet['tid'], tweet['uid'], tweet['cen'], tweet['t'], tweet['impact']))
		except MySQLdb.IntegrityError,e:
			#in case of duplicate tweets, we ignore the later ones
			continue
		except ValueError,e:
			continue
	db.commit()
'''

datapath = '/home/ubuntu/ccteam/backend/db/q3_testdata'
pos_file = codecs.open("q3_positive.csv","w+",encoding="utf8")
neg_file = codecs.open("q3_negative.csv","w+",encoding="utf8")

for i in os.listdir(datapath):
	path = datapath + '/' + i
	data_file = open(path)
	for line in data_file.readlines():
		try:
			tweet = json.loads(line)
			# if tweet['impact'] == 0:
			# 	continue
			# else:
			text = {}
			text['text'] = tweet['cen']
			json_text = json.dumps(text,ensure_ascii=False)
			csvline = str(tweet['tid']) + '\t' + str(tweet['uid']) + '\t' + json_text + '\t' + tweet['t'] + '\t' + str(tweet['impact'])
			csvline += '\n'
			if tweet['impact'] > 0:
				pos_file.write(csvline)
			else:
				neg_file.write(csvline)
				
		except ValueError,e:
			continue

	print "Finish Handlig " + i + "..."

pos_file.close()
neg_file.close()
