import MySQLdb as mdb
import re
from datetime import datetime, timedelta
from PIL import Image
import os.path
import sys

def func():
	conn = mdb.connect(host='137.132.145.238', user='fashion', passwd='lmsi3229fashion', db='fashion4_ads_test_1')
	cur = conn.cursor()
	
	sql = "select id, photourl, datepost, photourlhash from photocache limit 4000000"
	cur.execute(sql)
	rows = []
	rows = cur.fetchall()
	print len(rows)
	to = datetime.now().date() - timedelta(days=1)
	print to
	notice_counter = 0
	counter = 0
	less_counter = 0
	err_counter  = 0
	#print " before for"

	fi_err = open('err_url.file', 'w')
	fi_snq = open('snq_url.file', 'w')

	for row in rows:
		notice_counter += 1
		if notice_counter % 100000 == 0:
			print notice_counter
		if row[2] < to:
			file_path = '/public2/ads/image_test/original_image_folder/' + row[3][0:3] + "/" + row[3] + ".jpg"
			if os.path.exists(file_path):
				try:
					im = Image.open(file_path)
				except:
					#print file_path
					fi_err.write(str(row))
					fi_err.write("\n")
					fi_err.flush()
					err_counter += 1
					continue
				if im.mode != "RGB":
					im = im.convert("RGB")
				width, height = im.size
				if width * 1.0 / height > 3.5 or height * 1.0 / width > 3.5:
					counter += 1
					fi_snq.write(str(row) + " " + str(width) + " " + str(height))
					fi_snq.write("\n")
					fi_snq.flush()
			else:
				less_counter += 1

	print "counter: " + str(counter) + " , less_counter: " + str(less_counter) + ", err_counter: " + str(err_counter)

func()
