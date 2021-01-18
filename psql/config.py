import psycopg2


countries = ["Poland", "Iceland", "Neverland"]
cities = ["Bialystok", "Somewhere", "Nowhere"]


def get_conn():
	return psycopg2.connect(host='localhost', user='postgres', password='pass')


def db_exec(cursor, q, d, conn):
	cursor.execute(q, d)
	conn.commit()
