import redis
import time
import socket
import struct
import random

import config as conf
from config import connect_redis


def ps_subscribe(conn):
	sub = conn.pubsub(ignore_subscribe_messages=True)
	sub.subscribe(conf.two_to_three_ps)
	# barrier.wait()
	return sub


def run_proc_3():
	print("Start proc 3")

	conn = connect_redis()
	sub = ps_subscribe(conn)

	while 1:
		req_id = int(conn.blpop(conf.one_to_three_q, 0)[1])

		decision = random.randint(1, 10)

		if 1 <= decision <= 8:
			conn.hset("req_" + str(req_id), 'time_3', time.time())

		elif 9 == decision:
			continue

		else:
			for mess in sub.listen():
				if int(mess['data']) == req_id:
					conn.hset("req_" + str(req_id), 'time_3', time.time())
					break

	print("END proc 3")
