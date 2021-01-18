import psycopg2
import time
import select
import random

import config as conf


def pop_q_1_to_2(cursor, conn):
	q = """
		UPDATE queue_1_to_2
		SET status='taken'
		WHERE req_id IN (
		    SELECT req_id
		    FROM queue_1_to_2
		    WHERE status='new'
		    LIMIT 1
		    FOR NO KEY UPDATE SKIP LOCKED
		) RETURNING req_id;
		"""

	cursor.execute(q)
	rows = cursor.fetchall()
	conn.commit()

	if len(rows) < 1:
		return None

	return rows[0][0]


def notify_proc_3(cursor, req_id, conn):
	cursor.execute('NOTIFY "two_to_three", %s', (str(req_id), ))

	conn.commit()


def is_notified(conn):
	if conn.notifies:
		return
	while 1:
		if select.select([conn],[],[],0.5) == ([],[],[]):
			continue

		conn.poll()

		if conn.notifies:
			return


def run_proc_2():
	print("Start proc 2")

	conn = conf.get_conn()
	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	cursor = conn.cursor()

	cursor.execute('LISTEN q2;')

	while 1:
		is_notified(conn)

		req_id = pop_q_1_to_2(cursor, conn)

		if req_id is None:
			continue

		idx = random.randint(0, 2)

		q = """ UPDATE ads_info
				SET  country = %s, city = %s, time_2 = NOW()
				WHERE req_id = %s"""
		d = (conf.countries[idx], conf.cities[idx], req_id)
		conf.db_exec(cursor, q, d, conn)

		notify_proc_3(cursor, req_id, conn)


	print("END proc 2")