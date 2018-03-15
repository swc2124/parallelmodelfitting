# ====================================================================
# Author 				: swc21
# Date 					: 2018-03-14 09:41:45
# Project 				: ClusterFiles
# File Name 			: bestfit
# Last Modified by 		: swc21
# Last Modified time 	: 2018-03-14 10:57:52
# ====================================================================

#--[IMPORTS]------------------------------------------------------------------#
from mpi4py import MPI
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
HEAD_DISTANCE = 'BPI-M1-01'
HEAD_BOXSIZES = 'BPI-M1-09'
MIXER = 'Wolf-16'
#--[MAIN]---------------------------------------------------------------------#
if rank <= 2:
    from random import shuffle
    # LIST GENORATORS
    if rank <= 1:
        import itertools
        from numpy import load
        p_1 = '[HEAD] ['+str(rank)+'] ['+str(name)+'] '
        total_sent = 0
        len_lists_to_send = 5000
        # DISTANCE LISTS
        if rank == 0:
            # RUN PROGRAM
            distance_list = load(distance_list_path)
            shuffle(distance_list)
            shuffle(distance_list)
            dists = []
            last_list = None
            for x in itertools.permutations(distance_list):
                dists.append(x)
                list_length = len(dists)
                if list_length >= len_lists_to_send:
                    if last_list == dists:
                        print ERROR_MESSAGE
                        MPI.Abort()
                    d = comm.recv(source=2, tag=1)
                    if d:
                        total_sent += list_length
                        comm.send([dists, (rank, name)], dest=2, tag=2)
                        p_2 = '[SENDING JOB TO ' + \
                            str(d[0])+' ON '+str(d[1])+'] '
                        p_3 = '[t_sent '+str(total_sent)+'] '
                        last_list = dists
                        dists = []
                        p_4 = '[list_len was ' + \
                            str(list_length)+'| now : '+str(len(dists))+']'
                        print p_1+p_2+p_3+p_4
                    else:
                        print p_1+'[NO ONE TO SEND TO]'
            # SHUTDOWN PROGRAM
            d = comm.recv(source=2, tag=1)
            if d:
                comm.send(['KILL_1', (rank, name)], dest=2, tag=2)
            print EXIT_MESSAGE
            exit(0)
        # BOXSIZE LISTS
        else:
            # RUN PROGRAM
            boxsize_list = load(boxsize_list_path)
            shuffle(boxsize_list)
            shuffle(boxsize_list)
            bsizes = []
            last_list = None
            for b in itertools.permutations(boxsize_list):
                bsizes.append(b)
                list_length = len(bsizes)
                if list_length >= len_lists_to_send:
                    if last_list == bsizes:
                        print ERROR_MESSAGE
                        MPI.Abort()
                    d = comm.recv(source=2, tag=3)
                    if d:
                        total_sent += list_length
                        comm.send([bsizes, (rank, name)], dest=2, tag=4)
                        p_2 = '[SENDING JOB TO ' + \
                            str(d[0])+' ON '+str(d[1])+'] '
                        p_3 = '[t_sent '+str(total_sent)+'] '
                        last_list = bsizes
                        bsizes = []
                        p_4 = '[list_len was ' + \
                            str(list_length)+'| now : '+str(len(bsizes))+']'
                        print p_1+p_2+p_3+p_4
                    else:
                        print p_1+'[NO ONE TO SEND TO]'
            # SHUTDOWN PROGRAM
            d = comm.recv(source=2, tag=3)
            if d:
                comm.send(['KILL_2', (rank, name)], dest=2, tag=4)
            print EXIT_MESSAGE
            exit(0)
    # LIST MIXER AND SENDER
    else:
        def get_qlen(q):
            return len(q)
        p_1 = '[MIXER] ['+str(rank)+'] ['+str(name)+'] '
        sent = 0
        recv = 0
        RUN = True
        KILL_CMDS = []
        dists_queue = []
        bsize_queue = []
        queue = []
        while RUN == True:
            if len(KILL_CMDS) == 0:
                if get_qlen(queue) < 1000:
                    waiting_on_distances = True
                    waiting_on_box_sizes = True
                    comm.send([rank, name], dest=0, tag=1)
                    while waiting_on_distances == True:
                        dists, source = comm.recv(source=0, tag=2)
                        if dists:
                            p_3 = ' FROM RANK ' + \
                                str(source[0])+' ON HOST '+str(source[1])+'] '
                            if dists == 'KILL_1':
                                KILL_CMDS.append(dists)
                                p_2 = '< '+dists+' >'
                            else:
                                p_2 = 'DISTANCES'
                            message = '[REVIEVING '+p_2+p_3
                            if p_2 == 'DISTANCES':
                                print message
                                for distance_list in dists:
                                    dists_queue.append(distance_list)
                                    recv += 1
                            else:
                                print message
                            waiting_on_distances = False
                    print p_1+'-->[DISTANCES][DONE]'
                    comm.send([rank, name], dest=1, tag=3)
                    while waiting_on_box_sizes == True:
                        boxes, source = comm.recv(source=1, tag=4)
                        if boxes:
                            p_3 = ' FROM RANK ' + \
                                str(source[0])+' ON HOST '+str(source[1])+'] '
                            if boxes == 'KILL_2':
                                KILL_CMDS.append(boxes)
                                p_2 = '< '+boxes+' >'
                            else:
                                p_2 = 'BOX-SIZES'
                            message = '[REVIEVING '+p_2+p_3
                            if p_2 == 'BOX-SIZES':
                                print message
                                for bsize_list in boxes:
                                    bsize_queue.append(bsize_list)
                                    recv += 1
                            else:
                                print message
                            waiting_on_box_sizes = False
                    print p_1+'-->[BOX-SIZES][DONE]'
                    if len(dists_queue) == len(bsize_queue):
                        print p_1+'[ZIPPING QUEUE]'
                        while len(dists_queue) > 0:
                            queue.append(
                                zip(dists_queue.pop(), bsize_queue.pop()))
                        dists_queue = []
                        bsize_queue = []
                        print p_1+'[DONE]'
                    else:
                        print ERROR_MESSAGE
                        # SHUTDOWN WORKERS DUE TO ERROR
                        for i in range(size-3):
                            dest, name = comm.recv(
                                source=MPI.ANY_SOURCE, tag=5)
                            if d:
                                print p_1+'[KILLING RANK '+str(dest)+' ON HOST '+str(name)+'] '
                                comm.send(['KILL', (rank, name)],
                                          dest=dest, tag=6)
                    shuffle(queue)
                    print p_1+'[QUEUE LOADED] [SENDING JOBS NOW]'
                while get_qlen(queue) > 0:
                    jobs = [queue.pop() for i in range(100)]
                    dest, name = comm.recv(source=MPI.ANY_SOURCE, tag=5)
                    if dest:
                        print p_1+'SENDING JOBS TO RANK '+str(dest)+' ON HOST '+str(name)+'] '
                        comm.send([jobs, (rank, name)], dest=dest, tag=6)
                        jobs = []
            elif len(KILL_CMDS) == 2:
                RUN = False
                print p_1+'[BOTH KILL CMDS REVIEVED]'
            else:
                print ERROR_MESSAGE
        # SHUTDOWN PROGRAM
        print EXIT_MESSAGE
        exit(0)
else:
    from numpy import load, save
    part_1 = '[WORKER] ['+str(rank)+'] ['+str(name)+'] '
    time_array = load(time_array_path)
    nstars_array = load(nstars_array_path)
    k = 0
    high_score = 0
    best_job = None
    last_job = None
    max_time = 1050.0
    min_time = 500.0
    RUN = True
    while RUN == True:
        comm.send([rank, name], 2, tag=5)
        jobs, source = comm.recv(source=2, tag=6)
        if jobs == 'KILL':
            RUN = False
        else:
            for job in jobs:
                job.sort()
                if last_job == job:
                    continue
                    # print ERROR_MESSAGE+'[last_job==job]'
                    # comm.Abort()
                k += 1
                time = 0
                for distance_index, box_index in job:
                    time += time_array[distance_index, box_index]
                    if time > max_time:
                        break
                if time > max_time:
                    print part_1+'[TIMED OUT] [job '+str(k)+' ] [', round(time, 2), ' hrs]'
                elif time < min_time:
                    print part_1+'[TIMED OUT] [job '+str(k)+' ] [', round(time, 2), ' hrs]'
                else:
                    score = 0
                    print '[WORKER] ['+str(rank)+'] ['+str(name)+'] [COMPLETED] [job '+str(k)+'] [', round(time, 2), ' hrs]'
                    for distance_index, box_index in job:
                        score += nstars_array[distance_index:,
                                              :box_index].sum()
                    new_score = round(score/time*1e2, 2)
                    if new_score > high_score:
                        high_score = new_score
                        best_job = job
                        save(bestjob_path+'['+str(rank)+']bestjob', best_job)
                        print '\n--------------------------------------------------------------------------------------'
                        print part_1+'[new high score] [', int(high_score), ' stars/hr] ['+str(k)+' jobs done]'
                        print '--------------------------------------------------------------------------------------\n'
                last_job = job
print EXIT_MESSAGE
exit(0)
#-----------------------------------------------------------------------------#
