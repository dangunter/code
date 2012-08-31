#!/usr/bin/env python
"""
Put out a summary of Mongo doc structure
"""
import json
import pymongo
from bson import json_util
import re
import sys
import argparse

#### IO ####

def read_from_mongo(host=None, port=None, user=None, password=None,
                    database=None, collection=None):
    """Connect to MongoDB with given parameters and read the
    first entry in the given database/collection.
    
    Returns: dict
    """
    import pymongo
    try:
        mongo = pymongo.Connection(host=host, port=port)
    except Exception, err:
        print("Error connecting to {host}:{port:d}: {e}".
                    format(e=err, **conn))
        return -1
    db = mongo[database]
    auth_ok = db.authenticate(user, password)
    if not auth_ok:
        print("MongoDB authentication failed (db={db}, user={user}, pass={passwd})"
              .format(db=database, user=user, passwd=password))
        return -1
    coll = db[collection]
    return coll.find_one()

def read_from_file(f):
    """Read JSON data from file object `f`.

    Returns: dict
    """
    return json.load(f, object_hook=json_util.object_hook)

#### DOT ####

def dot_join(*lines):
    s = ""
    for line in lines:
        s += line
        if not line.endswith("{"):
            s += ";\n"
    return s

def dot_clean_label(s):
    return s.replace(" ","_").replace("(","_").replace(")","_")

def _dot(obj, parent, depth, param):
    if depth >= param['max_depth']:
        return
    if isinstance(obj, dict):
        for k,v in obj.iteritems():
            # skip dict/lists like { '1':'foo', '2':'bar', ... }
            if not param['dict_lists'] and re.match(r'\d+', k) and k != '1':
                continue 
            name = parent + "_" + dot_clean_label(k)
            label = k[:20] if param['show_key'] else "L{0:d}".format(depth)
            yield dot_join("{0} [label=\"{1}\"]".format(name, label),
                          "{0} -> {1}".format(parent, name))
            for text in _dot(v, name, depth+1, param):
                yield text
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            name = parent + "_{0:d}".format(i)
            label = "list()"
            yield dot_join("{0} [label=\"{1}\"]".format(name, label),
                          "{0} -> {1}".format(parent, name))
            for text in _dot(item, name, depth+1, param):
                yield text
    else:
        pass # terminate recursion

def dot_print(ofile, data, shape=None, aspect_ratio=None, **kw):
    """Print in DOT format
    """
    root_name = "root"
    ofile.write(dot_join(
        "digraph doc {",
        "graph [ratio = {0:f}]".format(aspect_ratio),
        "edge [arrowhead = none]",
        "node [shape = {0}]".format(shape),
        "L0 [label=\"{0}\"]".format(root_name)))
    for text in _dot(data, "L0", 1, kw):
        ofile.write(text)
    ofile.write("}\n")

#### HTML ####

def html_print(ofile, data, **kw):
    ofile.write("<HTML>\n")
    for text in _html(data, 0, kw):
        ofile.write(text)
    ofile.write("</HTML>\n")

def _html(obj, depth, param):
    if depth >= param['max_depth']:
        yield "&nbsp;..."
    elif isinstance(obj, dict):
        yield "<ul>"
        for k in sorted(obj.keys()):
            if not param['dict_lists'] and re.match(r'\d+', k) and k != '1':
                continue 
            v = obj[k]
            yield "<li>{key}".format(key=k)
            for text in _html(v, depth+1, param):
                yield text
            yield "</li>"
        yield "</ul>"
    elif isinstance(obj, list):
        if param['show_lists']:
            yield "<ul>"
            for i, item in enumerate(obj):
                yield "<li>[{n}]".format(n=i)
                for text in _html(item, depth+1, param):
                    yield text
                yield "</li>"
            yield "</ul>"
        else:
            yield "[]"
            if len(obj) > 0:
                for text in _html(obj[0], depth+1, param):
                    yield text
    else:
        pass
    
#### UTIL ####

def dedup(item):
    """Remove all but first item of lists contained in input.
    """
    if isinstance(item, dict):
        return {k:dedup(v) for k,v in item.iteritems()}
    elif isinstance(item, list) and len(item) > 0:
        return [dedup(item[0])]
    else:
        return item

#### MAIN ####

def main():
    # Arguments
    parser = argparse.ArgumentParser(
        description="Print structure for a MongoDB collection")
    parser.add_argument("mode", metavar="MODE",
                        help="Output mode: 'dot' or 'html'", default='dot')
    parser.add_argument("--file", dest="ifile", default=None,
                        help="Input file (default: stdin)")
    parser.add_argument("--mongo", dest="conn", default="",
                        help="Input MongoDB, host:port:user:pass:db:coll")
    parser.add_argument("--lists", dest="show_lists", action="store_true",
                        help="Show all list elements (default: first one)")
    parser.add_argument("--dict-lists", dest="show_dict_lists", action="store_true",
                        help="Show list elements from dicts with strictly "
                        "numeric keys (default: first one)")                    
    parser.add_argument("--depth", dest="maxdepth", type=int, default=99999,
                        help="Max. depth to show (default: all)")                
    parser.add_argument("--dot-labels", action="store_true", dest="is_full",
                        help="For DOT output, whether to show labels")    
    parser.add_argument("--dot-aspect", action="store", dest="ar", type=float,
                        help="For DOT output, aspect ratio of graph",
                        default=1.0)
    parser.add_argument("--dot-shape", action="store", dest="shp",
                        help="For DOT output, node shape",
                        default="box")
    args = parser.parse_args()
    mode = args.mode.lower()
    # Read data
    if args.conn:
        try:
            p = args.conn.split(':')
            conn = dict(host=p[0], port=int(p[1]),
                        user=p[2], password=p[3],
                        database=p[4], collection=p[5])
        except (ValueError, IndexError), err:
            parser.error("Bad format for --conn argument")
        data = read_from_mongo(**conn)
    else:
        if args.ifile is None:
            f = sys.stdin
        else:
            f = open(args.ifile)
        data = read_from_file(f)
    # Preprocess data
    data = dedup(data)
    # Write output
    ofile = sys.stdout
    if mode == 'dot':
        dot_print(ofile, data, show_key=args.is_full, aspect_ratio=args.ar,
                  shape=args.shp, show_lists=args.show_lists,
                  dict_lists=args.show_dict_lists,
                  max_depth=args.maxdepth)
    elif mode == 'html':
        html_print(ofile, data, show_lists=args.show_lists,
                   dict_lists=args.show_dict_lists,
                   max_depth=args.maxdepth)
    else:
        parser.error("Bad mode {}".format(mode))
    return 0
    
if __name__ == "__main__":
    sys.exit(main())
