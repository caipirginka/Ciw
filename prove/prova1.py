import ciw

def arrival_times(t):
	if t is None : return 0.0    #config check only
	res = 1.0
	if t == 0: res = 1.0
	if t == 1: res = 1.0
	elif t == 2: res = 1.0
	print u'time {} => {}'.format(t,res)
	return res


def arrival_sizes(t):
	if t is None : return 0.0    #config check only
	res = 1.0
	if t == 1: res = 2.0
	elif t == 2: res = 1.0
	elif t == 3: res = 1.0
	print u'size {} => {}'.format(t,res)
	return res

n = 10
N = ciw.create_network(
Arrival_distributions=[['TimeDependent', arrival_times]],
Service_distributions=[['Deterministic', 1.0]],
Batching_distributions=[['TimeDependent', arrival_sizes]],
Number_of_servers=[2],
Queue_capacities=['Inf']
)

ciw.seed(1)
Q = ciw.Simulation(N)
Q.simulate_until_max_time(n)
recs = Q.get_all_records()
