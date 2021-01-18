import redis
import time
import socket
import struct
import random

import config as conf
from config import connect_redis

script = '''
redis.call('HSET', ARGV[1], 'country', ARGV[2], 'city', ARGV[3], 'time_2', ARGV[4])
redis.call('PUBLISH', KEYS[1], KEYS[2])
'''


def run_proc_2():
	print("Start proc 2")

	conn = connect_redis()
	registered_script = conn.register_script(script)


	while 1:
		req_id = int(conn.blpop(conf.one_to_two_q, 0)[1])

		idx = random.randint(0, 2)

		registered_script(args=['req_'+str(req_id), conf.countries[idx],
								 conf.cities[idx], time.time()],
							keys=[conf.two_to_three_ps, req_id])

	print("END proc 2")


# run_proc_2()