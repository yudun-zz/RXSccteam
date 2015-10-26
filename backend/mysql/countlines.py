import os

datapath = '/home/osboxes/Developer/ccteamproj/phase1/smalldata/ou6'
lines_cnt = 0
for i in os.listdir(datapath):
	f = open(datapath + '/' + i)
	lines_cnt += len(f.readlines())
print lines_cnt
