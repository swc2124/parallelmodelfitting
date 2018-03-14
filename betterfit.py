# ====================================================================
# Author 				: swc21
# Date 					: 2018-03-14 09:41:45
# Project 				: ClusterFiles
# File Name 			: betterfit
# Last Modified by 		: swc21
# Last Modified time 	: 2018-03-14 11:00:22
# ====================================================================

#--[IMPORTS]------------------------------------------------------------------#
from mpi4py import MPI
from time import sleep
#--[PROGRAM-OPTIONS]----------------------------------------------------------#
# MPI params
comm = MPI.COMM_WORLD
mpisize = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()
# path params
path = '/root/bestfit/'
distance_list_path = path+'distance_list.npy'
boxsize_list_path = path+'boxsize_list.npy'
time_array_path = path+'time.npy'
nstars_array_path = path+'nstars.npy'
bestjob_path = '/root/SHARED/bestfit/bestfit_output/'
#--[FUNCTIONS]----------------------------------------------------------------#
#--[OPTIONS]------------------------------------------------------------------#
EXIT_MESSAGE = ' X --> [EXIT(0)] ['+str(rank)+'] ['+str(name)+'] '
ERROR_MESSAGE = ' X --> [ERROR] ['+str(rank)+'] ['+str(name)+'] '
HEAD = 0
#--[MAIN]---------------------------------------------------------------------#
if rank == HEAD:
    HEAD_HONCHO = name
else:
    HEAD_HONCHO = None
HEAD_HONCHO = comm.bcast(HEAD_HONCHO, HEAD)
comm.Barrier()
if rank == HEAD:
    print name
    import itertools
    from numpy import load
    p_1 = '[HEAD] ['+str(rank)+'] ['+str(name)+'] '
    total_sent = 0
    len_lists_to_send = 100
    distance_list = load(distance_list_path)
    boxsize_list = load(boxsize_list_path)
    dists = []
    for x in itertools.permutations(boxsize_list, len(distance_list)):
        dists.append(zip(distance_list, x))
        if len(dists) > 5000:
            while len(dists) > 0:
                d = comm.recv(source=MPI.ANY_SOURCE)
                list_length = len(dists)
                sending = []
                for i in range(len_lists_to_send):
                    if len(dists) == 0:
                        break
                    sending.append(dists.pop())
                    total_sent += 1
                comm.send([sending, (rank, name)], dest=d[0])
                p_2 = '[SENDING JOB TO '+str(d[0])+' ON '+str(d[1])+'] '
                p_3 = '[t_sent '+str(total_sent)+'] '
                p_4 = '[list_len was ' + \
                    str(list_length)+'| now : '+str(len(dists))+']'
                print p_1+p_2+p_3+p_4
        else:
            continue
    # SHUTDOWN PROGRAM
    for i in range(size-1):
        d = comm.recv(source=MPI.ANY_SOURCE)
        comm.send(['KILL', (rank, name)], dest=d[0])
    print EXIT_MESSAGE
    exit(0)
else:
    sleep(60)
    sleep(rank)
    if name == HEAD_HONCHO:
        print '[EXITING] ', name, rank
        exit(0)
    else:
        print name, rank
    from numpy import load, save
    part_1 = '[WORKER] ['+str(rank)+'] ['+str(name)+'] '
    time_array = load(time_array_path)
    nstars_array = load(nstars_array_path)
    k = 0
    high_score = 0
    best_job = None
    max_time = 1050.0
    min_time = 500.0
    RUN = True
    while RUN == True:
        comm.send([rank, name], dest=HEAD)
        jobs, source = comm.recv(source=HEAD)
        if jobs == 'KILL':
            RUN = False
        else:
            for job in jobs:
                k += 1
                time = 0
                for distance_index, box_index in job:
                    time += time_array[distance_index, box_index]
                    if time > max_time:
                        break
                if time > max_time:
                    # print part_1+'[TIMED OUT] [job '+str(k)+' ] [',round(time,2),' hrs]'
                    continue
                elif time < min_time:
                    # print part_1+'[TIMED OUT] [job '+str(k)+' ] [',round(time,2),' hrs]'
                    continue
                else:
                    score = 0
                    for distance_index, box_index in job:
                        score += nstars_array[distance_index:,
                                              :box_index].sum()
                    new_score = round(score/time*1e2, 2)
                    if new_score > high_score:
                        high_score = new_score
                        best_job = job
                        save(bestjob_path+'['+str(rank)+']bestjob', best_job)
                        print '\n--------------------------------------------------------------------------------------'
                        print part_1+'[new high score] [', int(high_score), ' stars/hr] ['+str(k)+' jobs done] [', round(time, 2), ' hrs]'
                        print '--------------------------------------------------------------------------------------\n'
            print '[WORKER] ['+str(rank)+'] ['+str(name)+'] [COMPLETED] [job '+str(k)+'] [', round(time, 2), ' hrs]'
print EXIT_MESSAGE
exit(0)
#-----------------------------------------------------------------------------#
