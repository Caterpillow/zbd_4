import redis


port_nr = 6379
one_to_two_q = "one_to_two_q"
one_to_three_q = "one_to_three_q"
two_to_three_ps = "two_to_three_ps"

countries = ["Poland", "Iceland", "Neverland"]
cities = ["Bialystok", "Somewhere", "Nowhere"]

def connect_redis():
	return redis.Redis(host='localhost', port=port_nr, db=0)