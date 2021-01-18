import config as conf


def new_db(conn):
	cursor = conn.cursor()

	cursor.execute("""DROP TABLE IF EXISTS ads_info""")
	cursor.execute("""DROP TABLE IF EXISTS queue_1_to_2""")
	cursor.execute("""DROP TABLE IF EXISTS queue_1_to_3""")

	cursor.execute("""CREATE TABLE ads_info
				 (req_id integer UNIQUE, cookie varchar, ip varchar, 
				 time_1 timestamp, country varchar, city varchar,
				 time_2 timestamp, time_3 timestamp)""")

	cursor.execute("""CREATE TABLE queue_1_to_2(
				  	req_id integer PRIMARY KEY,
				  	status VARCHAR(10) DEFAULT 'new'
					);""")

	cursor.execute("""CREATE TABLE queue_1_to_3(
			  	req_id integer PRIMARY KEY,
			  	status VARCHAR(10) DEFAULT 'new'
				);""")

	cursor.execute("""CREATE OR REPLACE RULE "req_notify_2" AS
			        ON INSERT TO queue_1_to_2 DO 
			        NOTIFY "q2";
			    		""")

	cursor.execute("""CREATE OR REPLACE RULE "req_notify_3" AS
		        ON INSERT TO queue_1_to_3 DO 
		        NOTIFY "q3";
		    		""")

	conn.commit()


def main():
	conn = conf.get_conn()
	new_db(conn)

	


main()
