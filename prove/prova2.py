import sys
import collections
import datetime
import dateutil.parser
import ciw
import timeit

t0 = timeit.default_timer()

u"""
Nodes definition where each one has a name and a capacity in terms of number of pieces.
The capacity may also be an array of two-items array to specify servers shifts, as explained in Ciw documentation.
Each Node must also have a queue length in terms of number of pieces (a negative number means infinite: 'Inf').
"""
nodes = collections.OrderedDict()
nodes['Pizzeria'] = {
#    'capacity': [[8,50.0],[2,100.0]],
    'capacity': 4,
#    'queue': 10                     #do NOT use it!!!
}
nodes['Cucina'] = {
    'capacity': 2,
    #'queue': -1                     #do NOT use it!!!
}

u"""
Categories definition where each one has a name, a Node where it must enter the system, a pruction time and a capacity weight (as an integer).
If a Category is heavier than another, produced at the same Node, their weights and the capacity of the Node, must be adjusted accordingly.
For example if a Panino use half the space of a Pizza inside the oven, the weight of Panino must be 1, the weight of Pizza must be 2
and the capacity of Node Pizzeria must be 8 (assuming the oven can accomodate 4 Pizzas at a time).
The production time must be the number of minutes needed to produce a single piece of the Category.
"""
categories = collections.OrderedDict()
categories['Pizze'] = {
    'node': 'Pizzeria',
    'time': 3,
    'weight': 2
}
categories['Panini'] = {
    'node': 'Pizzeria',
    'time': 4.0,
    'weight': 1
}
categories['Primi'] = {
    'node': 'Cucina',
    'time': 15.0,
    'weight': 1
}

u"""
Set the beginning and ending timestamp between which order pick-ups may be requested.
Set the timestamp when production can actually start.
"""
openstamp = datetime.datetime.now()                             #for testing, always accept orders pick-ups starting from now
closestamp = openstamp + datetime.timedelta(minutes = 120)      #close after two hours and refuse later order pick-ups
minstamp = openstamp + datetime.timedelta(minutes = -20)        #actual production starts 20 minutes before first order pick-up

u"""
Orders to simulate, where each one has a timestamp where it must be ready for pick up and list of ordered items.
To facilitate testing, the timestamp is calculated as number of minutes after the start of the simulation.
Each ordered item has the Category and the ordered quantity.
Orders does not need to be in any particular order.
"""
orders = [
    {
        'duestamp': (openstamp + datetime.timedelta(minutes = 0)).isoformat(),
        'items': [
            {
                'category': 'Primi',
                'qty': 4
            },
        ]
    },
    {
        'duestamp': (openstamp + datetime.timedelta(minutes = 30)).isoformat(),
        'items': [
            {
                'category': 'Pizze',
                'qty': 8
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
        'duestamp': (openstamp + datetime.timedelta(minutes = 20)).isoformat(),
        'items': [
            {
                'category': 'Panini',
                'qty': 2
            },
        ]
    },
    {
        'duestamp': (openstamp + datetime.timedelta(minutes = 29)).isoformat(),
        'items': [
            {
                'category': 'Panini',
                'qty': 4
            },
            {
                'category': 'Primi',
                'qty': 1
            }
        ]
    },
    {
        'duestamp': (openstamp + datetime.timedelta(minutes = 180)).isoformat(),
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

NOorders = [
    {
        'duestamp': (openstamp + datetime.timedelta(minutes = 0)).isoformat(),
        'items': [
            {
                'category': 'Panini',
                'qty': 10
            }
        ]
    },
    {
        'duestamp': (openstamp + datetime.timedelta(minutes = 9)).isoformat(),
        'items': [
            {
                'category': 'Panini',
                'qty': 1
            }
        ]
    }
]

def create_batch_func(clss):
    u"""
    Return a time dependent batch function for the specified customer class
    """
    piece = pieces.get(clss,None)           #get a reference to the ordered pieces for the specified customer class

    def _func(t):
        u"""
        Look into the enclosing ordered pieces for anything that must enter the system at the specified time.
        If not found, return 0 (no pieces must enter the system)
        """
        #print 'batch {}'.format(t)
        res = (piece.get(t,0) if piece else 0) * 1.0            #ensure float
        if res:
            print 'batch {}: {} => {}'.format(t,clss,res)
        return res
    return _func

#will contain the available production power at each Node
NodePower = collections.namedtuple('NodePower', 'capacity time count')
powers = [NodePower(sys.maxint,0.0,0.0) if v_node.get('capacity',-1) < 0 else NodePower(v_node['capacity'],0.0,0.0) for k_node,v_node in nodes.iteritems()]
arrivals = collections.OrderedDict()            #arrivals ditributions for each customer class
services = collections.OrderedDict()            #services ditributions for each customer class
transitions = collections.OrderedDict()         #transition matrices for each customer class
batches = collections.OrderedDict()             #batch ditributions for each customer class
servers = ['Inf' if v_node.get('capacity',-1) < 0 else v_node['capacity'] for k_node,v_node in nodes.iteritems()]    #available servers at each Node
queues = ['Inf' if v_node.get('queue',-1) < 0 else v_node['queue'] for k_node,v_node in nodes.iteritems()]           #max queue lentgh at each Node
pieces = collections.OrderedDict()              #quantity of orderd pieces for each customer class

u"""
Create a customer class for each Category and prepare all relevant dictionaries.
Inside each dictionary, prepare an array for each Node.
Arrival distribution are always deterministic at one minute step precision.
Service and batch distribution are relevant only for the Node where each Category must enter the system,
while in all other cases they are set to zero.
Service ditributions are deterministic as the number of minutes needed to produce each Category.
Batch ditributions use a time dependent function that gives the number of pieces that must enter the system at any minute interval step.
"""
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
            #add to the power of the Node for current Category the weighted time of the Category and increment the Category counter
            power = powers[i_node]
            powers[i_node] = NodePower(power.capacity,power.time + v_category['time'] * v_category['weight'],power.count + 1)
            services[clss].append(['Deterministic', v_category['time'] * 1.0])
            batches[clss].append(['TimeDependent', create_batch_func(k_category)])
        else:            
            services[clss].append(['Deterministic', 0.0])
            batches[clss].append(['Deterministic', 0.0])
        i_node = i_node + 1
    i_category = i_category + 1

total = 0                           #total number of ordered pieces that enter the system
begintime = sys.maxint              #will be the minimum simulation time where the first ordered piece must enter the system
duetime = 0                         #will be the maximum simulation time where the last ordered piece must exit the system

u"""
For each item of each order, calculate and store the number of pieces that must enter the system 
and the simulation time when they must enter.
The number of pieces is calculated by multiplying the ordered quantity by the weight of the item's Category.
The simulation time is calculated by subtracting the production time of the item's Category from the requested pick-up timestamp (duestamp).
The requested pick-up timestamp is tansformed to simulation time by calculating its difference in minutes 
from the minimum accepted orders pick-up timestamp (openstamp).
WARN!!! If it's negative, the order must be rejected!!!
Inside the "pieces" dictionary there's an entry for each Category that had at least one item ordered, where the key is the Category name
and the value is a dictionary where each key is the simulation time where some piece must enter the system and the value is the number of pieces
that must enter.
Also the minimum and maximum needed simulation time are calculated and stored.
"""
for order in orders:
    for item in order['items']:
        category = categories[item['category']]
        delta = int(round((dateutil.parser.parse(order['duestamp']) - openstamp).total_seconds() / 60))
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

u"""
If the minimum simulation time is negative, it means that the simulation must begin earlier than what was foresaw
(this can happen if an ordered item was in a Category with a long production time).
If it is positive, it means that the simulation may start later (this can happen if no order is due very soon).
To cope with this, all simulation enter time for all ordered pieces must be moved in the future or in the past,
so that their relative simulation enter time is preserved.
"""
if begintime != 0:
    for piece in pieces.values():
        for key in piece.keys():
            piece[key - begintime] = piece[key]
            del piece[key]

u"""
All simulation time moments must be moved accordingly to the actual simulation time instant.
Also calculate and store the simulation time corresponding to the minimum and maxint acceptable order pick-up timestamp.
"""
opentime = 0 - begintime
closetime = (closestamp - openstamp).total_seconds() / 60  - begintime
duetime = duetime - begintime

u"""
Calculate and store the actual timestamps for simulation start end and for last requested order pick-up.
The simulation must begin when the first piece must go into production.
"""
beginstamp = openstamp + datetime.timedelta(minutes=begintime)
duestamp = openstamp + datetime.timedelta(minutes=duetime)

print 'powers: {}'.format(powers)
print 'arrivals: {}'.format(arrivals)
print 'services: {}'.format(services)
print 'transitions: {}'.format(transitions)
print 'batches: {}'.format(batches)
print 'servers: {}'.format(servers)
print 'queues: {}'.format(queues)
print 'pieces: {}'.format(pieces)
print 'total: {}'.format(total)
print 'begintime: {}'.format(begintime)
print 'opentime: {}'.format(opentime)
print 'duetime: {}'.format(duetime)
print 'closetime: {}'.format(closetime)
print 'openstamp: {}'.format(openstamp.isoformat())
print 'closestamp: {}'.format(closestamp.isoformat())
print 'minstamp: {}'.format(minstamp.isoformat())
#if the simulation was started before the timestamp when production can actually start (minstamp),
#it means that the ordered items are not actually producible by the system.
print 'beginstamp: {} => {}'.format(beginstamp.isoformat(),'OK' if beginstamp >= minstamp else 'KO')
#if the simulation must end after the timestamp when the last order puck-up is acceptable (closestamp),
#it means that the ordered items are not actually producible by the system.
print 'duestamp: {} => {}'.format(duestamp.isoformat(),'OK' if duestamp <= closestamp else 'KO')

t1 = timeit.default_timer()

N = ciw.create_network(
    Arrival_distributions=arrivals,
    Service_distributions=services,
    Transition_matrices=transitions,
    Batching_distributions=batches,
    Number_of_servers=servers,
    Queue_capacities=queues
)

t2 = timeit.default_timer()

#exit()

ciw.seed(1)             #non random simulation

t3 = timeit.default_timer()

Q = ciw.Simulation(N)

t4 = timeit.default_timer()

#simulate until ALL ordered pieces are produced.
#This MUST NOT be used if queues are not of infinite length 'Inf', otherwise it may never end!!!
#Q.simulate_until_max_customers(total, method='Finish')

#simulate just until is needed (with one minute more).
#This can be used if queues are not of infinite length 'Inf'!!!
#Q.simulate_until_max_time(duetime + 1)                  

#simulate until the last order pick-Up was acceptable (with one minute more).
#This can be used if queues are not of infinite length 'Inf'!!!
Q.simulate_until_max_time(closetime + 1)

t5 = timeit.default_timer()

print Q.nodes[-1].number_of_individuals                 #number of pieces in exit node
#if it's not equal to the total number of ordered pieces, it means that the ordered items are not actually producible by the system,
#because their production could not end before requested pick-up time
print Q.nodes[-1].number_of_individuals == total        
recs = Q.get_all_records()
print t1 - t0
print t2 - t1
print t3 - t2
print t4 - t3
print t5 - t4

NodeResult = collections.namedtuple('NodeResult', 'load count meanwait maxwait countlate meanlate maxlate')
slots = []
noderesults = []
slottime = opentime
slotsize = 10.0
while slottime < closetime:
    slotstamp = (beginstamp + datetime.timedelta(minutes = slottime)).isoformat()
    print '{} => {}'.format(slottime,slotstamp)
    i_node = 1                  #Node 0 is always the ArrivalNode
    for k_node,v_node in nodes.iteritems():
        def _in_timeslot(rec,want_late):
            duedate = rec.arrival_date + rec.service_time
            was_late = rec.exit_date >= slottime + slotsize
            return rec.node == i_node and duedate >= slottime and duedate < slottime + slotsize and was_late == want_late
            
        results = [rec for rec in recs if _in_timeslot(rec,False)]
        count = len(results) * 1.0
        waits = [res.waiting_time for res in results]
        meanwait = sum(waits) / count if waits else 0.0
        maxwait = max(waits) if waits else 0.0
        #load = meanwait / slotsize

        results = [rec for rec in recs if _in_timeslot(rec,True)]
        countlate = len(results) * 1.0
        lates = [res.exit_date - (res.arrival_date + res.service_time) for res in results]
        countlate = len(lates) * 1.0
        meanlate = sum(lates) / countlate if lates else 0.0
        maxlate = max(lates) if lates else 0.0

        power = powers[i_node - 1]                  #there's no ArrivalNode here
        #load = (count + countlate) / ((power.capacity * slotsize) / (power.time / power.count))
        load = count / ((power.capacity * slotsize) / (power.time / power.count))
        
        noderesults.append(NodeResult(load,count,meanwait,maxwait,countlate,meanlate,maxlate))
        i_node = i_node + 1
        print '   {}: {} ({} {} {} {} {} {})'.format(k_node,load,count,meanwait,maxwait,countlate,meanlate,maxlate)
    slottime = slottime + slotsize

l = len(recs)
print 'number of recs: {}'.format(l)
if l <= 30:
    for r in recs:
        print r
else:
    print '...omissis...'

for k_clss,v_clss in Q.rejection_dict.iteritems():
    for k_node,v_node in v_clss.iteritems():
        l = len(v_node)
        print 'number of rejections for Class {} at Node {}: {}'.format(k_clss,k_node,l)
        if l <= 30:
            print v_node
        else:
            print '...omissis...'
