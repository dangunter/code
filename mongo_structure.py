#!/usr/bin/env python
"""
Put out a summary of Mongo doc structure
"""
import json
import pymongo
from bson import json_util
import sys
import argparse

g_ifile = sys.stdin
g_ofile = sys.stdout

SHOW_KEY = False
ASPECT_RATIO = 1.0
MODE_DOT, MODE_CSV = 0,1
SHAPE = "box"

def _dot(*a):
    for s in a:
        g_ofile.write(s)
        if not s.endswith("{"):
            g_ofile.write(";\n")

def main(mode, conn):
    if conn:
        import pymongo
        try:
            mongo = pymongo.Connection(host=conn['host'], port=conn['port'])
        except Exception, err:
            print("Error connecting to {host}:{port:d}: {e}".
                        format(e=err, **conn))
            return -1
        db = mongo[conn['db']]
        auth_ok = db.authenticate(conn['user'], conn['passwd'])
        if not auth_ok:
            print("MongoDB authentication failed (db={db}, user={user}, pass={passwd})"
                  .format(**conn))
            return -1
        coll = db[conn['coll']]
        d = coll.find_one()
    else:
        d = json.load(g_ifile, object_hook=json_util.object_hook)
    root_name = "root"
    if mode == MODE_DOT:
        shape = SHAPE #("point", "box")[SHOW_KEY]
        _dot("digraph doc {",
             "graph [ratio = {0:f}]".format(ASPECT_RATIO),
             "edge [arrowhead = none]",
             "node [shape = {0}]".format(shape),
             "L0 [label=\"{0}\"]".format(root_name))
        print_dot(d, "L0", 1)
        g_ofile.write("}\n")
    elif mode == MODE_CSV:
        print "CSV not implemented. Har, har!"
        return

def print_dot(obj, parent, depth):
    for k,v in obj.iteritems():
        name = parent + "_" + clean_label(k)
        if SHOW_KEY:
            label = k[:20]
        else:
            label = "L{0:d}".format(depth)
        _dot("{0} [label=\"{1}\"]".format(name, label),
             "{0} -> {1}".format(parent, name))
        if isinstance(v, dict):
            print_dot(v, name, depth+1)
        elif isinstance(v, list):
            for i, v2 in enumerate(v):
                if isinstance(v2, dict):
                    num = "n{0:d}".format(i)
                    item_name = name + num
                    if SHOW_KEY:
                        label = (k + num)[:20]
                    else:
                        label = "L{0:d}l".format(depth)
                    _dot("{0} [label=\"{1}\"]".format(item_name, label),
                         "{0} -> {1}".format(parent, item_name))
                    print_dot(v2, item_name, depth+1)

def clean_label(s):
    return s.replace(" ","_").replace("(","_").replace(")","_")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Print structure for a MongoDB collection")
    parser.add_argument("mode", metavar="MODE",
                        help="Output mode: 'dot' or 'csv'", default='dot')
    parser.add_argument("--conn", dest="conn", default="",
                        help="Connection host:port:user:pass:db:coll")
    parser.add_argument("--dot-labels", action="store_true", dest="is_full",
                        help="For DOT output, whether to show labels")    
    parser.add_argument("--dot-aspect", action="store", dest="ar", type=float,
                        help="For DOT output, aspect ratio of graph",
                        default=1.0)
    parser.add_argument("--dot-shape", action="store", dest="shp",
                        help="For DOT output, node shape",
                        default=SHAPE)    
    args = parser.parse_args()
    mode = args.mode.lower()[:3]
    if mode not in ('dot', 'csv'):
        parser.error("Bad value for MODE")
    mode = {'dot':MODE_DOT, 'csv':MODE_CSV}[mode]
    SHOW_KEY = args.is_full
    ASPECT_RATIO = args.ar
    SHAPE = args.shp
    if args.conn:
        try:
            p = args.conn.split(':')
            conn = dict(host=p[0], port=int(p[1]),
                        user=p[2], passwd=p[3],
                        db=p[4], coll=p[5])
        except (ValueError, IndexError), err:
            parser.error("Bad format for --conn argument")
    else:
        conn = None
    main(mode, conn)