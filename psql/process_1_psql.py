import psycopg2
import time
import random
import string
import socket
import struct

import config as conf


ads_number = 1000 * 1000 * 100


def gen_cookie_ip():
	cookie = ''.join(random.choices(string.ascii_uppercase + string.digits, 
									k=10))
	ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
	return cookie, ip



def run_proc_1():
	print("Start proc 1")

	conn = conf.get_conn()
	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	cursor = conn.cursor()	

	# barrier.wait()

	for _ in range (0, ads_number):
		req_id = random.randint(1, 1000 * 1000 * 1000)

		cookie, ip = gen_cookie_ip()

		q = """ INSERT INTO ads_info (req_id, cookie, ip, time_1)
				VALUES (%s, %s, %s, NOW())"""
		d = (req_id, cookie, ip)
		conf.db_exec(cursor, q, d, conn)

		q = """ INSERT INTO queue_1_to_2 (req_id)
				VALUES (%s)"""
		d = (req_id,)
		conf.db_exec(cursor, q, d, conn)

		q = """ INSERT INTO queue_1_to_3 (req_id)
				VALUES (%s)"""
		d = (req_id,)
		conf.db_exec(cursor, q, d, conn)

	conn.commit()

	print("END proc 1")