items_to_subtract = ['Shuffle Write Time', 'Fetch Wait Time'] # ,"TSQR"]#, 'JVM GC Time']
del_keys = ["Launch Time", "Finish Time", "JVM GC Time", "Time To Finish From Stage Start", "Getting Result Time"] 

def update_task_dict(task_info, appStart, stage_dic, algo, stage_to_phase_dict, accumulables):
    '''Parse task dict for time related keys
    Add Total Task Time, Task Start Delay and Time until stage end to dict
    Run tests and Add Compute bucket using stage_to_phase_dict'''
    
    stage_id = task_info["Stage ID"]
    stage_info = get_stage_info(stage_dic, stage_id)
    desired_dict = parse_dict(task_info, algo, accumulables)
    desired_dict = add_new_members(task_info, desired_dict, stage_info, algo)
    run_tests(desired_dict, stage_info)
    desired_dict = delete_items(desired_dict)
    desired_dict = create_compute_bucket(stage_id,desired_dict, algo, stage_dic, stage_to_phase_dict,accumulables)
    
    #for some strange reasons some tasks in the same stage have an accumulable and some don't (very strange indeed)
    if accumulables is not None:
        if accumulables['key_name'] not in desired_dict:
            desired_dict[accumulables['key_name']] = 0.0
    
    #convert to seconds
    for k, v in desired_dict.iteritems():
        desired_dict[k] = v / 1000.
    return desired_dict


def delete_items(desired_dict):

    for k in del_keys:
        del desired_dict[k]
    return desired_dict
        

def add_new_members(task_info, desired_dict,stage_info, algo):
    stage_name, stage_start, stage_end = stage_info
    task_accted_for_time = desired_dict['Executor Run Time'] + desired_dict['Executor Deserialize Time'] + \
                           desired_dict['Result Serialization Time']  + desired_dict['Getting Result Time']
    total_task_time = desired_dict['Finish Time'] - desired_dict['Launch Time']
    

#     if algo == "nmf":
#         if "TSQR" not in desired_dict:
#             desired_dict["TSQR"] = 0.0
#         if desired_dict["TSQR"] > desired_dict["Executor Run Time"]:
#             assert False, "%s" % (desired_dict)
    desired_dict['Scheduler Delay'] = total_task_time - task_accted_for_time
    desired_dict['Time To Finish From Stage Start'] = desired_dict['Finish Time'] - stage_start
    desired_dict['Task Start Delay'] = desired_dict['Launch Time'] - stage_start
        
    desired_dict['Time Waiting Until Stage End'] = stage_end - desired_dict['Finish Time']
    
    return desired_dict
    
def run_tests(desired_dict, stage_info):
    #assert False
    stage_name, stage_start, stage_end = stage_info
    total_task_time = desired_dict['Finish Time'] - desired_dict['Launch Time']
    assert desired_dict['Task Start Delay'] +  total_task_time == desired_dict['Time To Finish From Stage Start']
    assert desired_dict['Time To Finish From Stage Start'] + desired_dict['Time Waiting Until Stage End'] == stage_end -    stage_start
    sum_ = 0
    for k in ['Executor Run Time',
              'Getting Result Time',
              'Driver Compute Time',
              'Task Start Delay',
              'Result Serialization Time',
              'Scheduler Delay',
              'Executor Deserialize Time']:

        if k in desired_dict:
            sum_ += desired_dict[k]
        
    assert sum_ == desired_dict['Time To Finish From Stage Start']
#     for k, v in desired_dict.iteritems():
#         assert v >= 0, "Oh no key %s is %d" % (k,v)

def run_task_tests(pd,maxes_for_mpl):
    maxes_sum = sum([maxes_for_mpl[k] for k in ordered_keys  if k in maxes_for_mpl and k!= 'Driver Compute Time' and  k!= 'Time Waiting Until Stage End'])
    maxes_sum - maxes_for_mpl['Time To Finish From Stage Start']
    assert np.abs(maxes_sum - maxes_for_mpl['Time To Finish From Stage Start']) < 0.1
    total_app_time = maxes_for_mpl['Time To Finish From Stage Start'] + maxes_for_mpl['Driver Compute Time'] + maxes_for_mpl['Time Waiting Until Stage End'] 
    assert total_app_time ==  pd.appEnd - pd.appStart
    
def get_stage_info(stage_dic, stage_id):
    stage_name = stage_dic[stage_id]['name']
    stage_start = stage_dic[stage_id]['stage_submit_time']
    stage_end =  stage_dic[stage_id]['stage_complete_time']
    return stage_name, stage_start, stage_end
    
def parse_dict(task_info, algo, accumulables):
    '''get all time related k,value pairs in the task dict from the json event logs
    this is recurtsive b/c we parse dicts inside dicts'''
    desired_dict={}
    for key,value in task_info.iteritems():
        if "Time" in str(key):
            if "Shuffle Write Time" in key:
                value /= 1000000.0
            desired_dict[str(key)] = value
        
        #If we specify we want to track an accumulable
        if key == "Accumulables":
            if accumulables is not None and len(value) > 1:
                
                for d in value:
                    if d['Name'] == accumulables['accum_name']: #"Time taken for Local QR":
                        desired_dict[accumulables['key_name']] = float(d['Update'])
        if isinstance(value,dict):
            d = parse_dict(value, algo, accumulables)
            desired_dict.update(d)

                

   


    return desired_dict

def flat_map_dict(key_plus_dic):
    '''returns a list of nested tuples, where each tuple is 
    ((key[0], key[1],...key[n], dic_key[i]), value[i])'''
    key, dic = key_plus_dic
    key = [key] if not isinstance(key, tuple) or not isinstance(key,list) else list(key)
    return [(tuple(key + [dic_key]),v) for dic_key,v in dic.iteritems()]

def create_compute_bucket(stage_id,dic, algo, stage_dict, stage_to_phase_dict, accumulables):
    
    stage_name = str(stage_dict[stage_id]['name'].split(':')[0])
    val = stage_to_phase_dict[stage_name]
    if isinstance(val, dict):
        if stage_id in val:
            compute_bucket_name = stage_to_phase_dict[stage_name][stage_id]
        else:
            compute_bucket_name = stage_to_phase_dict[stage_name]['the_rest']
    else:
        compute_bucket_name = stage_to_phase_dict[stage_name]

        


    

    dic[compute_bucket_name] = dic['Executor Run Time']
    del dic['Executor Run Time']
    it2s = items_to_subtract
    if accumulables is not None:
        it2s = it2s + [accumulables['key_name']]
    for k in dic.keys():
        if k in items_to_subtract:
            try:
                dic[compute_bucket_name] -= dic[k]
            except:
                print  "%s %s" % (k, str(dic[k]))
    return dic