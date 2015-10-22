#!/usr/bin/env python
from __future__ import division
import socket
import time
import datetime
import cql
import os

host = os.environ.get('HOST', 'localhost')
port = os.environ.get('HOST', '9160')
ks = os.environ.get('KEYSPACE', 'operations')

def get_id(w):
    return '-'.join(w.split('-')[-2:])

con = cql.connect('localhost', int(port), ks, cql_version='3.1.0')
cur = con.cursor()

res = cur.execute('SELECT nodename, repair_status, data_center, WRITETIME(repair_status) FROM %s.repair_status' % ks)
now = datetime.datetime.now()
me = get_id(socket.gethostname())
if res:
    results = cur.fetchall()
    for t in results:
        node, st, dc, wt = t
        id_ = get_id(node)

        if st != 'Completed':
            # 0002/231/256
            _, step, total = st.split('/')
            perc = int(int(step) * 100 / int(total))
        else:
            perc = 100

        wt = datetime.datetime.fromtimestamp(wt/1000000)
        enlapsed = now - wt
        print("node %s status %s in dc %s (%s%%) last updated %s (%s)" % (node, st, dc, perc, wt.strftime('%Y-%m-%d %H:%M:%S'), enlapsed))
