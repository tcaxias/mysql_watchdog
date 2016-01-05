#!/usr/bin/python2

from __future__ import print_function
from mysql.connector import connect as con, errors as err
from socket import socket, AF_INET, SOCK_STREAM
from math import log
from subprocess import call
from time import sleep, time
from os import listdir as ls, stat, getpid
from stat import S_ISSOCK as is_socket
from sys import stderr
from multiprocessing import Pool

check_time = 2
main_port = 3308
max_time = 60
sock_dir = '/var/run/mysqld/'

my_cnf = {
    'option_files' : '/root/.my.cnf',
    'raw' : False
}

def nl(a):
    return a+'\n'

def get_sockets(dir=sock_dir):
    return [ s for s in ls(dir) if is_socket(stat(dir + s).st_mode) ]

def p_err(*objs):
    print("[" + str(getpid()) + "]", *objs, file=stderr)

def lag_to_percent(lag,max_time=max_time):
    return 100 * log(max_time-lag,max_time)

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
        db_data=cur.fetchone()
        cur.close()
        return db_data
    except Exception,e:
        p_err(e, '->', sql)
        return False

def get_master_status(cnx):
    db_data=do_cur(cnx,"""select count(*)
        from information_schema.processlist where user='repl'""")
    if isinstance(db_data, type(None)):
        return 0
    if db_data[0]>1:
        return 'up 100%'
    return 'down'

def get_galera_status(cnx):
    db_data=do_cur(cnx,"""select variable_value
        from information_schema.global_status
        where variable_name='wsrep_local_state'""")
    if isinstance(db_data, type(None)):
        return 0
    if db_data[0]=='4':
        return 'up 100%'
    return 'down'

def get_slave_status(cnx):
#    db_data=do_cur(cnx,"show all slaves status")
    db_data=do_cur(cnx,"show slave status",True)
    if isinstance(db_data, type(None)):
        return 0
    if isinstance(db_data['Seconds_Behind_Master'],int):
        lag=db_data['Seconds_Behind_Master']
    else:
        return 'down'
    if lag<max_time:
        return 'up ' + str(int(lag_to_percent(lag))) + '%'
    return 'down'

def get_port(cnx):
    db_data=do_cur(cnx,"""
    select variable_value from information_schema.global_variables
    where variable_name='port';""")
    if isinstance(db_data, type(None)):
        return 0
    if int(db_data[0])>0:
        return int(db_data[0])
    return 0

def check_cycle(cnx):
    haproxy_str=get_slave_status(cnx)
    if not isinstance(haproxy_str,int):
        return haproxy_str
    haproxy_str=get_galera_status(cnx)
    if not isinstance(haproxy_str,int):
        return haproxy_str
    haproxy_str=get_master_status(cnx)
    if not isinstance(haproxy_str,int):
        return haproxy_str
    return 'up 100%'

def listen_tcp(port):
    try:
        serversocket = socket(AF_INET, SOCK_STREAM)
        serversocket.bind(('0.0.0.0', port))
    except Exception,e:
        p_err("port "+str(port)+" "+str(e))
        quit()
    else:
        p_err("Got port ",port,", starting to listen.")
    serversocket.listen(10)
    return serversocket

def spawn_monitor(my_cnf,main_port=main_port):
    haproxy_str = 'down'
    old_haproxy_str = 'down'
    ts = time()
    serversocket=0
    db_port = 0

    while True:
        while True:
            cnx=do_cnx(**my_cnf)
            if isinstance(cnx,bool):
                sleep(1)
            else:
                db_port = get_port(cnx)
                p_err(db_port)
                break

        try:
            serversocket.close()
        except:
            pass
        finally:
            serversocket = listen_tcp(db_port+2)

        while True:
            connection, address = serversocket.accept()
            if time()-ts > check_time:
                haproxy_str=check_cycle(cnx)
                if isinstance(haproxy_str,int):
                    connection.send(nl('down'))
                    connection.close()
                    break
                ts = time()
            connection.send(nl(haproxy_str))
            connection.close()
            if haproxy_str!=old_haproxy_str:
                p_err("Changed from",old_haproxy_str,"to",haproxy_str)
                old_haproxy_str=haproxy_str

if __name__ == '__main__':
    # MAIN
    try:
        sockets = map(lambda x : sock_dir + x, get_sockets())
        sockets[0]+'abc'
    except Exception,e:
        quit("No mysql sockets found in",sock_dir)
    else:
        p_err("Monitoring sockets",sockets)

    if len(sockets)==1:
        my_cnf['unix_socket'] = sockets[0]
        spawn_monitor(my_cnf)
    elif len(sockets)>1:
        serversocket = listen_tcp(main_port)
        p=Pool(len(sockets))
        p.map(spawn_monitor,
            [ dict(my_cnf,**{'unix_socket':s}) for s in sockets ])
    else:
        quit("Empty socket list")
