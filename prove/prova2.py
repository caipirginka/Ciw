import collections
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

orders = [
    {
        'duetime': 0,
        'items': [
            {
                'category': 'Primi',
                'qty': 4
            },
        ]
    },
    {
        'duetime': 30,
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
        'duetime': 20,
        'items': [
            {
                'category': 'Panini',
                'qty': 1
            },
        ]
    },
    {
        'duetime': 1800,
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

def create_arrival_func(k_category):
    tick = ticks.get(k_category,None)
    
    def _func(t):
        print 'arrival {}'.format(t)
        res = tick.get(t,0) if tick else duetime
        if not res:
            res = duetime if t > tick.keys()[-1] else 1
        res = res * 1.0            #ensure float
        if res and True:
            print 'arrival {}: {} => {}'.format(t,k_category,res)
        return res
    return _func

def create_batch_func(k_category):
    piece = pieces.get(k_category,None)

    def _func(t):
        #print 'batch {}'.format(t)
        res = piece.get(t,0) * 1.0 if piece else 0.0
        #res = piece.get((t or 0) + 1,0) * 1.0 if piece else 0.0
        if res and True:
            print 'batch {}: {} => {}'.format(t,k_category,res)
        return res
    return _func

def zero_func(t):
    return 0.0

def one_func(t):
    return 1.0

arrivals = collections.OrderedDict()
services = collections.OrderedDict()
transitions = collections.OrderedDict()
batches = collections.OrderedDict()
servers = [v_node['capacity'] for k_node,v_node in nodes.iteritems()]
pieces = collections.OrderedDict()
ticks = collections.OrderedDict()

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
    ticks[k_category] = collections.OrderedDict()
    i_node = 0
    for k_node,v_node in nodes.iteritems():
        #print k_node
        arrivals[clss].append(['Deterministic', 1.0])
        if v_category['node'] == k_node:
            #arrivals[clss].append(['TimeDependent', create_arrival_func(k_category)])
            services[clss].append(['Deterministic', v_category['time'] * 1.0])
            batches[clss].append(['TimeDependent', create_batch_func(k_category)])
        else:            
            #arrivals[clss].append(['TimeDependent', one_func])
            services[clss].append(['Deterministic', 0.0])
            batches[clss].append(['TimeDependent', zero_func])
        i_node = i_node + 1
    i_category = i_category + 1

total = 0
begintime = 0
duetime = 0
for order in orders:
    for item in order['items']:
        category = categories[item['category']]
        t = order['duetime'] - category['time'] - 1
        #print t
        piece = pieces[item['category']]
        qty = item['qty'] * category['weight']
        piece[t] = piece.get(t,0) + qty        
        total = total + qty
        if t < begintime:
            begintime = t
        if order['duetime'] > duetime:
            duetime = order['duetime']
        #print piece[t]

if begintime != 0:
    duetime = duetime - begintime
    for piece in pieces.values():
        for key in piece.keys():
            piece[key - begintime] = piece[key]
            del piece[key]

for k_tick,v_tick in ticks.iteritems():
    piece = sorted(pieces.get(k_tick,[]))
    prev = 0.0
    for tick in piece:
        v_tick[prev] = tick - prev - 1
        prev = tick
    
print arrivals
print services
print transitions
print batches
print servers
print pieces
print ticks
print total
print begintime
print duetime

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
#Q.simulate_until_max_customers(total, method='Arrive')

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
