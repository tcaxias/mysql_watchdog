#!/usr/bin/python2

from __future__ import print_function
from mysql.connector import connect as con, errors as err
from socket import socket, AF_INET, SOCK_STREAM
from math import log
from subprocess import call
from time import sleep, time
import sys

check_time = 5
port = 3308

my_cnf = {
    'option_files' : '/root/.my.cnf',
    'raw' : False,
    'unix_socket' : '/var/run/mysqld/mysqld.sock'
}

def p_err(*objs):
    print(*objs, file=sys.stderr)

def lag_to_percent(lag):
    return 100 * log(60-lag,60)

def do_cnx(**my_cnf):
    try:
        cnx = con(**my_cnf)
        return cnx
    except Exception,e:
        p_err(e)
        return False

def do_cur(cnx,sql,cur_rets_dict=False):
    try:
        cur = cnx.cursor(dictionary=cur_rets_dict)
        cur.execute(sql)
        db_data=[ row for row in cur.fetchall() ]
        cur.close()
        return db_data
    except Exception,e:
        p_err(e, '->', sql)
        return False

def get_master_status(cnx):
    db_data=do_cur(cnx,"select count(*) from information_schema.processlist where user='repl'")
    if isinstance(db_data, bool):
        return 0
    if db_data[0]>1:
        return 'up 100%'
    return 'down'

def get_galera_status(cnx):
    db_data=do_cur(cnx,"select variable_value from information_schema.global_status where variable_name='wsrep_local_state'")
    if isinstance(db_data, bool):
        return 0
    if db_data[0]==4:
        return 'up 100%'
    return 'down'

def get_slave_status(cnx):
#    db_data=do_cur(cnx,"show all slaves status")
    db_data=do_cur(cnx,"show slave status",True)
    if isinstance(db_data, bool):
        return 0
    if isinstance(db_data[0]['Seconds_Behind_Master'],int):
        lag=db_data[0]['Seconds_Behind_Master']
    else:
        return 'down'
    if lag<=60:
        return 'up ' + str(int(lag_to_percent(lag))) + '%'
    return 'down'

def check_cycle(cnx):
    haproxy_str=get_slave_status(cnx)
    if not isinstance(haproxy_str,int):
        return haproxy_str
    haproxy_str=get_galera_status(cnx)
    if not isinstance(haproxy_str,int):
        return haproxy_str
    return 'up 100%'

# MAIN
try:
    serversocket = socket(AF_INET, SOCK_STREAM)
    serversocket.bind(('0.0.0.0', port))
except Exception,e:
    quit(e)
else:
    p_err("Got port ",port,", starting to listen.")

serversocket.listen(10)

haproxy_str='down'
ts = time()

while True:
    while True:
        cnx=do_cnx(**my_cnf)
        if isinstance(cnx,bool):
            sleep(1)
        else:
            break

    while True:
        connection, address = serversocket.accept()
        if time()-ts > check_time:
            haproxy_str=check_cycle(cnx)
            if isinstance(haproxy_str,int):
                connection.send('down')
                connection.close()
                break
            ts = time()
        connection.send(haproxy_str)
        connection.close()
