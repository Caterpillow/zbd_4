import redis
import time
import socket
import struct
import random
import string


import config as conf
from config import connect_redis


ads_number = 1000 * 1000 * 100
# ads_number = 5

script = '''
redis.call('HSET', ARGV[1], 'req_id', ARGV[2], 'cookie', ARGV[3], 'ip', ARGV[4], 'time_1', ARGV[5])
redis.call('RPUSH', KEYS[1], ARGV[2])
redis.call('RPUSH', KEYS[2], ARGV[2])
'''


def gen_cookie_ip():
	cookie = ''.join(random.choices(string.ascii_uppercase + string.digits, 
									k=10))
	ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
	return cookie, ip


def run_proc_1():
	print("Start proc 1")

	conn = connect_redis()
	ps = conn.pubsub()

	registered_script = conn.register_script(script)

	# barrier.wait()

	for req_id in range (0, ads_number):
		cookie, ip = gen_cookie_ip()
		registered_script(keys=[conf.one_to_two_q, conf.one_to_three_q], 
					args=[f'req_{req_id}', req_id, cookie, ip, time.time()])

	print("END proc 1")

# run_proc_1()