import time
from multiprocessing import Process

from process_1 import run_proc_1
from process_2 import run_proc_2
from process_3 import run_proc_3

from config import connect_redis


proc_1_nr = 1
proc_2_nr = 4
proc_3_nr = 4

sleep_secs = 20

def run_processes():
	print(time.time())

	conn = connect_redis()
	conn.flushall()

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


	# time
	keys = conn.keys('req_*')
	for key in keys:
		time_dict = {proc_nr: float(conn.hget(key, 'time_' + str(proc_nr)) or '0') 
						for proc_nr in range(1, 4)}

		# print('%f  %f %f' % (time_dict[1], time_dict[2], time_dict[3]))
		if time_dict[3] - time_dict[1] > 0:
			print('1 to 2: %f  1 to 3: %f' % (time_dict[2] - time_dict[1],
										 time_dict[3] - time_dict[1]))

 

run_processes()

