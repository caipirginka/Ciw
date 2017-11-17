import sys
import collections
import datetime
import dateutil.parser
import ciw
import timeit

t0 = timeit.default_timer()

nodes = collections.OrderedDict()
nodes['Pizzeria'] = {
    'capacity': 8
}
nodes['Cucina'] = {
    'capacity': 2
}

categories = collections.OrderedDict()
categories['Pizze'] = {
    'node': 'Pizzeria',
    'time': 3,
    'weight': 2
}
categories['Panini'] = {
    'node': 'Pizzeria',
    'time': 5.0,
    'weight': 1
}
categories['Primi'] = {
    'node': 'Cucina',
    'time': 15.0,
    'weight': 1
}

nowstamp = datetime.datetime.now()
basestamp = nowstamp + datetime.timedelta(minutes = 60)         #set it to 10 to have beginstamp < nowstamp => impossible

orders = [
    {
        'duestamp': (basestamp + datetime.timedelta(minutes = 0)).isoformat(),
        'items': [
            {
                'category': 'Primi',
                'qty': 4
            },
        ]
    },
    {
        'duestamp': (basestamp + datetime.timedelta(minutes = 30)).isoformat(),
        'items': [
            {
                'category': 'Pizze',
                'qty': 2
            },
            {
                'category': 'Panini',
                'qty': 3
            },
            {
                'category': 'Primi',
                'qty': 1
            }
        ]
    },
    {
        'duestamp': (basestamp + datetime.timedelta(minutes = 20)).isoformat(),
        'items': [
            {
                'category': 'Panini',
                'qty': 1
            },
        ]
    },
    {
        'duestamp': (basestamp + datetime.timedelta(minutes = 180)).isoformat(),
        'items': [
            {
                'category': 'Panini',
                'qty': 1
            },
            {
                'category': 'Primi',
                'qty': 2
            }
        ]
    },
]

def create_batch_func(k_category):
    piece = pieces.get(k_category,None)

    def _func(t):
        #print 'batch {}'.format(t)
        res = (piece.get(t,0) if piece else 0) * 1.0            #ensure float
        if res:
            print 'batch {}: {} => {}'.format(t,k_category,res)
        return res
    return _func

def zero_func(t):
    return 0.0

arrivals = collections.OrderedDict()
services = collections.OrderedDict()
transitions = collections.OrderedDict()
batches = collections.OrderedDict()
servers = [v_node['capacity'] for k_node,v_node in nodes.iteritems()]
pieces = collections.OrderedDict()

i_category = 0
for k_category,v_category in categories.iteritems():
    clss = 'Class ' + str(i_category)
    #print k_category
    #print clss
    arrivals[clss] = []
    services[clss] = []
    transitions[clss] = [[0.0] * len(nodes)] * len(nodes)       #always go to exit node
    batches[clss] = []
    pieces[k_category] = collections.OrderedDict()
    i_node = 0
    for k_node,v_node in nodes.iteritems():
        #print k_node
        arrivals[clss].append(['Deterministic', 1.0])
        if v_category['node'] == k_node:
            services[clss].append(['Deterministic', v_category['time'] * 1.0])
            batches[clss].append(['TimeDependent', create_batch_func(k_category)])
        else:            
            services[clss].append(['Deterministic', 0.0])
            batches[clss].append(['TimeDependent', zero_func])
        i_node = i_node + 1
    i_category = i_category + 1

total = 0
begintime = sys.maxint
duetime = 0
for order in orders:
    for item in order['items']:
        category = categories[item['category']]
        delta = int(round((dateutil.parser.parse(order['duestamp']) - basestamp).total_seconds() / 60))
        t = delta - category['time'] - 1
        #print t
        piece = pieces[item['category']]
        qty = item['qty'] * category['weight']
        piece[t] = piece.get(t,0) + qty        
        total = total + qty
        if t < begintime:
            begintime = t
        if delta > duetime:
            duetime = delta
        #print piece[t]

if begintime != 0:
    duetime = duetime - begintime
    for piece in pieces.values():
        for key in piece.keys():
            piece[key - begintime] = piece[key]
            del piece[key]

beginstamp = basestamp + datetime.timedelta(minutes=begintime)
duestamp = basestamp + datetime.timedelta(minutes=duetime)

print 'arrivals: {}'.format(arrivals)
print 'services: {}'.format(services)
print 'transitions: {}'.format(transitions)
print 'batches: {}'.format(batches)
print 'servers: {}'.format(servers)
print 'pieces: {}'.format(pieces)
print 'total: {}'.format(total)
print 'begintime: {}'.format(begintime)
print 'duetime: {}'.format(duetime)
print 'nowstamp: {}'.format(nowstamp.isoformat())
print 'basestamp: {}'.format(basestamp.isoformat())
print 'beginstamp: {} => {}'.format(beginstamp.isoformat(),'OK' if beginstamp >= nowstamp else 'KO')
print 'duestamp: {}'.format(duestamp.isoformat())

t1 = timeit.default_timer()

N = ciw.create_network(
    Arrival_distributions=arrivals,
    Service_distributions=services,
    Transition_matrices=transitions,
    Batching_distributions=batches,
    Number_of_servers=servers
)

t2 = timeit.default_timer()

#exit()

ciw.seed(1)

t3 = timeit.default_timer()

Q = ciw.Simulation(N)

t4 = timeit.default_timer()

Q.simulate_until_max_time(duetime + 1)

t5 = timeit.default_timer()

print Q.nodes[-1].number_of_individuals
print Q.nodes[-1].number_of_individuals == total
recs = Q.get_all_records()
print t1 - t0
print t2 - t1
print t3 - t2
print t4 - t3
print t5 - t4
print len(recs)
for rec in recs:
    print rec
