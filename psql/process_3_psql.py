import psycopg2
import time
import psycopg2.extensions
import select
import random

import config as conf


def pop_q_1_to_3(cursor, conn):
	q = """
		UPDATE queue_1_to_3
		SET status='taken'
		WHERE req_id IN (
		    SELECT req_id
		    FROM queue_1_to_3
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


def update_time_3(cursor, conn, req_id):
	q = """ UPDATE ads_info
			SET time_3 = NOW()
			WHERE req_id = %s"""
	d = (req_id,)
	conf.db_exec(cursor, q, d, conn)


def is_notified(conn):
    if conn.notifies:
        return conn.notifies.pop(0)
    while 1:
        if select.select([conn],[],[],0.5) == ([],[],[]):
            continue
        conn.poll()
        if conn.notifies:
            return conn.notifies.pop(0)
    return None


def run_proc_3():
	print("Start proc 3")

	conn = conf.get_conn()
	conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	cursor = conn.cursor()	

	cursor.execute("LISTEN two_to_three;")
	cursor.execute("LISTEN q3;")

	ok = False

	while 1:
		notify = is_notified(conn)
		if notify is None:
			return
		if notify.channel != "q3":
			continue

		req_id = pop_q_1_to_3(cursor, conn)

		if req_id is None:
			continue

		decision = random.randint(1, 10)

		if 1 <= decision <= 8:
			update_time_3(cursor, conn, req_id)

		elif 9 == decision:
			continue

		else:
			while 1:
				notify = is_notified(conn)

				if notify is None:
					return
				if notify.channel != 'two_to_three':
					continue
				if notify.payload != str(req_id):
					continue

				update_time_3(cursor, conn, req_id)


	print("END proc 3")