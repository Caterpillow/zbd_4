import time
from multiprocessing import Process

from process_1_psql import run_proc_1
from process_2_psql import run_proc_2
from process_3_psql import run_proc_3

from datetime import timedelta

import config as conf


proc_1_nr = 1
proc_2_nr = 4
proc_3_nr = 4

sleep_secs = 20

def run_processes():
	print(time.time())

	proc_dict = dict()

	proc_1_list = [Process(target=run_proc_1, args=())
					for id in range(proc_1_nr)]

	proc_2_list = [Process(target=run_proc_2, args=())
					for id in range(proc_2_nr)]

	proc_3_list = [Process(target=run_proc_3, args=())
					for id in range(proc_3_nr)]

	proc_dict = {1:proc_1_list, 2:proc_2_list, 3:proc_3_list}

	#start
	for i in range(1, 4):
		for p in proc_dict[i]:
			p.start()

	time.sleep(sleep_secs)

	# firstly 2, 3, processes
	for i in range(2, 4):
		for p in proc_dict[i]:
			p.terminate()

	for p in proc_dict[1]:
		p.terminate()


	conn = conf.get_conn()
	cursor = conn.cursor()

	q = """SELECT time_3 - time_1 
			FROM ads_info WHERE time_3 is not null"""

	cursor.execute(q)
	rows = cursor.fetchall()

	time_dict = dict()


	for i in range(len(rows)):
		time_dict = {proc_nr: rows[i][proc_nr - 1]/ timedelta(microseconds=1000) for proc_nr in range(1, 3)}

		print('1 to 2: %f  1 to 3: %f' % (time_dict[1], time_dict[2]))


	print("Reklamy: %d" %(len(rows)))

 

run_processes()

