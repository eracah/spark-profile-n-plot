import sys
import json
import numpy as np
from operator import add
from math import sqrt
#from pyspark import SparkContext
#sc = SparkContext(appName="ParseEventLog")
from util import *
import copy


min_or_max_dict = {'min': np.argmin, 'max': np.argmax}

def get_stage_names(sc, event_log):
    stage_names = sc.textFile(event_log).filter(lambda line:"SparkListenerStageComplete" in line).map(lambda x: str(json.loads(x)["Stage Info"]["Stage Name"])).distinct().collect()
    return stage_names
    
    
#God class
class ParseLogs(object):
    def __init__(self, sc, event_log, algo, stage_to_phase_name_dict, 
                 kill_stages=None, accumulables=None, driver_comp_labels=None):
        
        self.driver_comp_labels = driver_comp_labels
        #accumulables = sc.broadcast(accumulables)
        '''kill stages is list of stage id's we want to ignore'''
        self.algo = algo
        self.stage_to_phase_dict = sc.broadcast(stage_to_phase_name_dict)  #{'cx': cx_dict, 'pca': pca_dict, 'nmf':nmf_dict})

        
        loglines = sc.textFile(event_log).filter(lambda line: '"Failed":true' not in line).cache()

            
        self.appStart, self.appEnd = [self.get_app_start(loglines), self.get_app_end(loglines)]

        self.stage_dict = sc.broadcast(self.make_stage_dict(loglines))
        self.num_execs = loglines.filter(lambda x: "ExecutorAdded" in x).count()
        
        self.driver_comp_time, self.driver_comp_dict, self.driver_comp_times = self.calc_driver_comp_time(self.stage_dict.value)
        
        self.run_time = (self.appEnd - self.appStart + self.driver_comp_times[0] + self.driver_comp_times[-1])
        self.stage_times = self.get_stage_times()
    
        #self.stage_keyed_task_buckets  = self.stage_keyed_task_info.flatMap(flat_map_dict).cache()
        
        
        self.stage_keyed_task_info, self.stage_and_exec_keyed_task_info = self.calc_stage_keyed_task_info(loglines,self.algo, self.stage_to_phase_dict,self.appStart, self.stage_dict, kill_stages, accumulables)
        
        self.task_counts_dict = sc.broadcast(self.stage_keyed_task_info.map(lambda (i,v): (i,1))\
                                                                      .reduceByKey(add)\
                                                                      .collectAsMap())
        self.run_time /= 1000.
    def calc_stage_keyed_task_info(self, loglines, algo, stage_to_phase_dict, appStart, stage_dict, kill_stages, accumulables):
        #we do this so we don't pass on the whole class to the executor (only really matters for nonlocal spark)
        stage_and_exec_keyed_task_info = loglines\
                            .filter(lambda x: "SparkListenerTaskEnd" in x)\
                            .map(lambda x: json.loads(x))\
                            .map(lambda taskInfo: 
                                 ( 
                                    (taskInfo["Stage ID"],
                                    int(taskInfo["Task Info"]["Executor ID"])),
                                    taskInfo )
                                )\
                .map(lambda (i,v): (i,update_task_dict(v, appStart, stage_dict.value, algo, stage_to_phase_dict.value, accumulables))).cache()
                #.map(lambda (i,v): (i,create_compute_bucket(i[0],v,algo, stage_dict.value,
                                                        #stage_to_phase_dict.value))).cache()
                

        if kill_stages:
            stage_and_exec_keyed_task_info = stage_and_exec_keyed_task_info\
                                                   .filter(lambda (i,v): i[0] not in kill_stages)
            self.run_time -= sum([self.stage_times[i] for i in kill_stages])
            
            
        '''an rdd of (stage_id, task_info_dictionary) tuples'''
        stage_keyed_task_info = stage_and_exec_keyed_task_info.map(lambda (i,v): (i[0],v)).cache()
        return stage_keyed_task_info, stage_and_exec_keyed_task_info
        
    def calc_driver_comp_time(self, stage_dict):
        start_finish_times = [(stage_dict[k]['stage_submit_time'], 
                               stage_dict[k]['stage_complete_time']) for k in range(len(stage_dict.keys())) ]
        driver_comp_times = [start_finish_times[i][0] - start_finish_times[i-1][1] for i,t in enumerate(start_finish_times) ]
        #overwrite the 0th b/c it does wraparound
        driver_comp_times[0] = start_finish_times[0][0] - self.appStart
        post_dist_driver_time = self.appEnd - start_finish_times[-1][1]
        driver_comp_times.append(post_dist_driver_time)
        driver_comp_dict = dict(zip(range(-1,len(start_finish_times) + 1),driver_comp_times))
        total_driver_comp_time = sum(driver_comp_times)
        return total_driver_comp_time, driver_comp_dict, driver_comp_times

    def get_stage_times(self):
        
        stage_times = {k: self.stage_dict.value[k]['stage_complete_time'] - self.stage_dict.value[k]['stage_submit_time']  for k in self.stage_dict.value.keys()}
        return stage_times
    
    def get_f_task_per_stage(self,f,stage_keyed_task_info, key):
        
        task_counts_dict = self.task_counts_dict
        if f == 'min':
            ret = stage_keyed_task_info.reduceByKey(lambda v1,v2: v1 if v1[key] < v2[key] else v2)
        elif f == 'max':
            ret = stage_keyed_task_info.reduceByKey(lambda v1,v2: v1 if v1[key] > v2[key] else v2)

        elif f == 'rand':
            ret = stage_keyed_task_info.reduceByKey(lambda v1,v2: v1 if np.random.rand() > 0.5 else v2)
        elif f == 'mean':
            ret = stage_keyed_task_info.reduceByKey(lambda v1,v2: {k: v1[k] + v2[k] for k in v1.keys()})\
            .map(lambda (i,v): (i,{k: v[k] / task_counts_dict.value[i] for k in v.keys()}))
        #TODO: make this work if executor is in key too
        elif f == 'median':
            ret = self.hacky_calc_median(stage_keyed_task_info, key)

        else:
            assert False, "must specify miin, max or median not %s" % f
        return ret
    
    

    def hacky_calc_median(self, stage_keyed_task_info, key ):
        '''this has some weird hacks: 
        1.collects all the values that fit the key for each stage
        2. locally computes median 
            (but appends an extra value to any lists that are even in lenght, so median will not be an avg of two )
        3. Finds all tasks for a given stage that have the value that matches 
                    median for that stage (ie if key is Total Task Time 
        and the median total task time for stage 4 is 50 then gets the task_info 
                dicts for all tasks in stage 4 with Total Task time of 50)
        4. Get rid of duplicates by picking at random one of the median tasks (TODO: get avg or something)
        '''   
        task_dict = stage_keyed_task_info.map(lambda (i,v): (i,v[key])).groupByKey().collectAsMap()
        stage_and_key_keyed_task_info = stage_keyed_task_info.map(lambda (i,v): ((i,v[key]),v))
        #duplicates max value in list to end to make all lists odd
        list_dict ={stage:list(task_value_iter) + ([max(list(task_value_iter))] 
                         if len(list(task_value_iter)) % 2 == 0 else []) 
                    for stage,task_value_iter in task_dict.iteritems()}
        med_dict = {k:int(np.median(v)) for k,v in list_dict.iteritems()}
        ret = stage_and_key_keyed_task_info\
            .map(lambda ((stage,median_v_key),v): (stage,med_dict[stage], 
                                                   median_v_key, v)).filter(lambda (stage,med,val,v): med==val)\
            .map(lambda (stage,med,val,v): (stage,v))\
            .reduceByKey(lambda v1,v2: v1)
        return ret


    def get_summary_tasks_for_each_executor(self,f, key, fold_task_overheads=True):
        if f == 'median':
            raise NotImplementedError

        f_tasks_for_each_stage_exec = self.get_f_task_per_stage(f,self.stage_and_exec_keyed_task_info, key)
        
        if fold_task_overheads:
            f_tasks_for_each_stage_exec = self.fold_into_task_overheads(task_sum_for_mpl)

        return f_tasks_for_each_stage_exec

    def get_summary_tasks_for_mpl(self,f, key,fold_task_overheads=True):
        '''formats the '''
        '''selects task bucket as key and ignores other keys like stage_num or executor ''' 
        task_sum_for_mpl = self.get_f_task_per_stage(f,self.stage_keyed_task_info, key).flatMap(flat_map_dict).map(lambda (i,v): (i[-1], v)).reduceByKey(add).collectAsMap()

        #add driver compute time
        task_sum_for_mpl = self.add_in_driver_time(task_sum_for_mpl)
            
        if fold_task_overheads:
            task_sum_for_mpl = self.fold_into_task_overheads(task_sum_for_mpl)
            
        return task_sum_for_mpl
    
    def add_in_driver_time(self, task_dict, driver_comp_labels=None):
        '''Move times from Driver Compute Time bucket to user specified name bucket'''
        task_dict.update({'Driver Compute Time': (self.driver_comp_time / 1000.) })
        if self.driver_comp_labels is not None:
            for k,v in self.driver_comp_labels.iteritems():
                if k not in task_dict:
                    task_dict[k] = self.driver_comp_times[v] / 1000.
                else:
                    task_dict[k] += self.driver_comp_times[v] / 1000.
                #sub
                task_dict['Driver Compute Time'] -= self.driver_comp_times[v] / 1000.
  
        return task_dict
    
    def fold_into_task_overheads(self, summary_dic):
        task_overhead_keys = [ "Fetch Wait Time", "Shuffle Write Time", "Result Serialization Time",
                            ] #Scheduler Delay, "Executor Deserialize Time", "Driver Compute Time"
    
        for key in task_overhead_keys:
            if "Task Overhead Time" not in summary_dic:
                summary_dic["Task Overhead Time"] = 0
                summary_dic["Task Overhead Time"] += summary_dic[key]
            else:
                summary_dic["Task Overhead Time"] += summary_dic[key]
            del summary_dic[key]
                
        return summary_dic
          



    def get_app_start(self,log_rdd):
        appStart = log_rdd.filter(lambda x: "SparkListenerApplicationStart" in x)\
                       .map(lambda x: int(json.loads(x)["Timestamp"])).take(1)[0]
        return appStart


    def get_app_end(self,log_rdd):

        appEnd = log_rdd.filter(lambda x: "SparkListenerApplicationEnd" in x)\
                       .map(lambda x: int(json.loads(x)["Timestamp"])).take(1)[0]
        return appEnd

    def make_stage_dict(self,log_rdd):
        stage_info = log_rdd.filter(lambda x: "SparkListenerStageCompleted" in x)\
                         .map(lambda x: json.loads(x)["Stage Info"])\
                         .map(lambda stageEvent: (stageEvent["Stage ID"],
                                     stageEvent["Stage Name"],int(stageEvent["Submission Time"]),
                                                             int(stageEvent["Completion Time"] )))
        stage_dict = {tup[0] : dict(zip([ 'name','stage_submit_time', 'stage_complete_time'],tup[1:])) for tup in stage_info.collect()}
        return stage_dict
    
    
#    def get_summary_buckets_for_mpl(self,f):
#        pass
#         f_buckets_per_stage = self.get_f_buckets_per_stage(f,self.stage_keyed_task_buckets)
#         bucket_sum_for_mpl = f_buckets_per_stage.reduceByKey(add).collectAsMap()

#         bucket_sum_for_mpl.update({'Driver Compute Time': self.driver_comp_time - self.driver_comp_times[-1]})
#         bucket_sum_for_mpl.update({self.algo_to_phase_dict[self.algo][-1] : self.driver_comp_times[-1]})
#         return bucket_sum_for_mpl
#        def get_f_buckets_per_stage(self,f,stage_keyed_task_buckets):
#         '''returns rdd of the min, mean, max or min bucket time for each stage'''
#         if f == "mean":
#             ret = stage_keyed_task_buckets.reduceByKey(add)\
#                                           .map(lambda (i,v): (i,float(v) / task_counts_dict.value[i[0]] ))
#         elif f == 'min':
#             ret = stage_keyed_task_buckets \
#                     .reduceByKey(min)
#         elif f == 'max':
#             ret = stage_keyed_task_buckets \
#                     .reduceByKey(max)
#         elif f == 'median':
#             ret = stage_keyed_task_buckets \
#                 .groupByKey().map(lambda (i,v): (i, np.median(list(v))))
#         #elif f == 'stdev':

#         return ret



#     def get_stdev_buckets_per_stage(self,stage_keyed_task_buckets, mean_dict, merge_stages=False):
#         stdevs = stage_keyed_task_buckets\
#                 .map(lambda (i,v): (i,(float(v) - b_means.value[i])**2)) \
#                 .reduceByKey(add) \
#                 .map(lambda (i,v): (i,sqrt(v) / float(task_counts_dict.value[i[0]]))) #.cache()

#         return stdevs
    
    
    



