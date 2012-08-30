#!/bin/sh
HOST=localhost
PORT=27017
if [ $# -ne 4 ]; then
    printf "usage: $0 user password database collection\n"
    exit 1
fi
user=$1
shift
passwd=$1
shift
db=$1
shift
coll=$1
shift
./mongo_structure.py --dot-labels --dot-shape=plaintext \
  --conn "$HOST:$PORT:$user:$passwd:$db:$coll" dot > \
  ${coll}.dot
twopi ${coll}.dot -Tpdf -o${coll}_twopi.pdf
