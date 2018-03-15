# ====================================================================
# Author 				: swc21
# Date 					: 2018-03-14 09:41:45
# Project 				: ClusterFiles
# File Name 			: evenbetterfit
# Last Modified by 		: swc21
# Last Modified time 	: 2018-03-14 11:00:16
# ====================================================================

#--[IMPORTS]------------------------------------------------------------------#
import sys

from mpi4py import MPI
from time import sleep
#--[PROGRAM-OPTIONS]----------------------------------------------------------#
send_amount = 5*1e4
# MPI params
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()
# path params
path = '/root/SHARED/bestfit/'
distance_list_path = path+'distance_list.npy'
boxsize_list_path = path+'boxsize_list.npy'
time_array_path = path+'time.npy'
nstars_array_path = path+'nstars.npy'
bestjob_path = '/root/SHARED/bestfit/bestfit_output/'
EXIT_MESSAGE = ' X --> [EXIT(0)] ['+str(rank)+'] ['+str(name)+'] '
ERROR_MESSAGE = ' X --> [ERROR] ['+str(rank)+'] ['+str(name)+'] '
VENDORS = [18, 25, 28, 29]
#--[FUNCTIONS]----------------------------------------------------------------#


def HONCHO(
        rank=rank,
        name=name,
        EXIT_MESSAGE=EXIT_MESSAGE,
        ERROR_MESSAGE=ERROR_MESSAGE):
    # print '[ HEAD-HONCO ] ['+str(rank)+'] ['+str(name)+'] [STARTING]'
    from numpy import load
    import itertools  # , sys
    distance_list = load(distance_list_path)
    boxsize_list = load(boxsize_list_path)
    total_sent = 0
    p_1 = '[ HEAD-HONCHO ] ['+str(rank)+'] ['+str(name)+'] '
    bsizes = []
    for x in itertools.permutations(boxsize_list, len(distance_list)):
        bsizes.append(x)
        length = len(bsizes)
        if length > send_amount:
            sys.stdout.write('\n')
            while len(bsizes) > 0:
                _rank_, _host_ = comm.recv(source=MPI.ANY_SOURCE)
                list_length = len(bsizes)
                total_sent += list_length
                comm.send(bsizes, dest=_rank_)
                sys.stdout.write('\r'+p_1+'[ '+str(total_sent)+' ] [ '+str(
                    length)+' ] [SENDING JOB TO '+str(_rank_)+' ON '+str(_host_)+'] ')
                bsizes = []
        else:
            if length % 10 == 0:
                sys.stdout.write(
                    '\r'+p_1+'[ '+str(total_sent)+' ] [ '+str(length)+' ]')
        sys.stdout.flush()
    # SHUTDOWN PROGRAM
    for i in range(size):
        if i == HEAD:
            continue
        d = comm.recv(source=MPI.ANY_SOURCE)
        comm.send(['KILL', (rank, name)], dest=d[0])
    exit(0)


def VENDOR(rank=rank, name=name, HEAD=0, EXIT_MESSAGE=EXIT_MESSAGE, ERROR_MESSAGE=ERROR_MESSAGE):
    from numpy import load
    sys.stdout.write('\n[ VENDOR ] ['+str(rank) +
                     '] ['+str(name)+'] [STARTING]\n')
    distance_list = load(distance_list_path)
    SHUTDOWN = False
    known_hosts = []
    shutdown_lst = []
    queue = []
    total_sent = 0
    total_recv = 0
    VEND_NUMBER = 2500
    while SHUTDOWN == False:
        p_1 = '\n[ VENDOR ] ['+str(rank)+'] ['+str(name) + \
            '] [hosts '+str(len(known_hosts))+'] '
        while len(queue) < VEND_NUMBER*10:
            comm.send([rank, name], dest=HEAD)
            boxes = comm.recv(source=HEAD)
            if boxes == 'KILL':
                SHUTDOWN = True
                queue = []
                break
            else:
                sys.stdout.write('\n')
                for boxlist in boxes:
                    queue.append(zip(distance_list, boxlist))
                    total_recv += 1
                    sys.stdout.write('\r[ VENDOR ] ['+str(rank)+'] ['+str(
                        name)+'] [ recv : '+str(total_recv)+'] [ sent : '+str(total_sent)+']')
                    sys.stdout.flush()
                sys.stdout.write('\n')
                sys.stdout.flush()
        while len(queue) > VEND_NUMBER:
            to_send = [queue.pop() for i in range(VEND_NUMBER)]
            total_sent += VEND_NUMBER
            dest, host = comm.recv(source=MPI.ANY_SOURCE)
            if not host in known_hosts:
                known_hosts.append(host)
                shutdown_lst.append(dest)
            comm.send([to_send, (rank, name)], dest=dest)
            p_2 = '[SENDING JOB TO '+str(dest)+' ON '+str(host)+']\n'
            sys.stdout.write(str('\n'+p_1+p_2))
            sys.stdout.flush()
    shutdown = []
    for d in shutdown_lst:
        dest, host = comm.recv(source=MPI.ANY_SOURCE)
        comm.send(['KILL', (rank, name)], dest=dest)
        shutdown.append(host)
    shutdown.sort()
    known_hosts.sort()
    if shutdown == known_hosts:
        # print EXIT_MESSAGE+'[ALL HOSTS SHUTDOWN]'
        # print shutdown
        exit(0)
    else:
        # print EXIT_MESSAGE+' --> [SOME HOSTS NOT SHUTDOWN]'
        exit(1)


#--[OPTIONS]------------------------------------------------------------------#
#--[MAIN]---------------------------------------------------------------------#
if rank == 0:
    HONCHO(rank=rank, name=name)
    sys.exit(0)
if rank in VENDORS:
    for i in range(len(VENDORS)):
        if VENDORS[i] == rank:
            sleep(i*20)
            VENDOR(rank=rank, name=name)
            sys.exit(0)
        else:
            continue
    sys.exit(0)
sleep(80)
if name in ['Wolf-01', 'BPI-M1-15', 'BPI-M1-14', 'BPI-M1-11', 'BPI-M1-04']:
    sleep(rank*0.9)
    sys.stdout.write(str('\n'+' >>> SHUTTING DOWN - rank : ' +
                         str(rank)+' on host : '+str(name)+' <<<'))
    sys.stdout.flush()
    sys.exit(0)
else:
    from numpy import load, save, asarray, array_split
    addresses = [i.tolist() for i in array_split(range(size), len(VENDORS))]
    assert len(addresses) == len(VENDORS)
    for i in range(len(addresses)):
        if rank in addresses[i]:
            address = VENDORS[i]
            vendor = str(address)
    time_array = load(time_array_path)
    nstars_array = load(nstars_array_path)
    p_1 = '[ WORKER ] ['+str(rank)+'] ['+str(name) + \
        '] [vendor('+vendor+')'+str(address)+'] '
    k = 0
    high_score = 0
    best_job = None
    max_time = 1150.0
    min_time = 700.0
    RUN = True
    sleep(rank*0.25)
    sys.stdout.write(str('\n'+p_1+'[ STARTING ]'))
    sys.stdout.flush()
    while RUN == True:
        sleep(rank)
        comm.send([rank, name], dest=address)
        jobs, source = comm.recv(source=address)
        vendor = source[1]
        p_1 = '[ WORKER ] ['+str(rank)+'] ['+str(name) + \
            '] [vendor('+vendor+')'+str(address)+'] '
        if jobs == 'KILL':
            RUN = False
        else:
            for job in jobs:
                k += 1
                time = 0
                for distance_index, box_index in job:
                    if time > max_time:
                        continue
                    else:
                        time += time_array[distance_index, box_index]
                if time > max_time:
                    sys.stdout.write(str(
                        '\n'+p_1+'[ TIMED-OUT MAX ] [ job '+str(k)+' ] [ '+str(round(time, 2))+' hrs ]'))
                    sys.stdout.flush()
                    continue
                elif time < min_time:
                    sys.stdout.write(str(
                        '\n'+p_1+'[ TIMED-OUT MIN ] [ job '+str(k)+' ] [ '+str(round(time, 2))+' hrs ]'))
                    sys.stdout.flush()
                    continue
                else:
                    new_score = 0
                    for distance_index, box_index in job:
                        new_score += box_index
                    if new_score > high_score:
                        high_score = new_score
                        best_job = job
                        save(
                            bestjob_path+'/area/['+str(new_score)+'][rank:'+str(rank)+']', best_job)
                        sys.stdout.write(str('\n'+p_1+'[ NEW-HIGH-SCORE ] [ '+str(
                            int(high_score))+' ] [ '+str(k)+' jobs done ] [ '+str(round(time, 2))+' hrs ]'))
                        sys.stdout.flush()
            sys.stdout.write(
                str('\n'+p_1+'[COMPLETED] [job '+str(k)+'] ['+str(round(time, 2))+' hrs]'))
            sys.stdout.flush()
    sys.exit(0)
#-----------------------------------------------------------------------------#
