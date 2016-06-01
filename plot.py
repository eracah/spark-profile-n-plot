import matplotlib
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors


class Plotter(object):
    def __init__(self, colors_list=None, colors_dict=None, mpi_or_spark='spark'):
        self.colors_list = colors_list
        if self.colors_list is None:
            self.colors_list = colors.cnames.keys()
        
        self.colors_dict = colors_dict
        self.mpi_or_spark = mpi_or_spark
    def _plot_bar_plot(self,items_to_plot, ordered_keys, colors_dict):

        plt.rcParams['figure.figsize'] = (20.0, 16.0)
       

        bottom = len(items_to_plot.values()) *[0]
        colors_ = colors.cnames.keys()
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        ind = np.arange(len(bottom))
        width = 0.7
        label_table = {}
        items_to_plot_v = [items_to_plot[k] for k in sorted(items_to_plot.keys())]
        for i,k in enumerate(ordered_keys):
            for j, it in enumerate(items_to_plot_v):
                if k not in it: #or k == "Time Waiting Until Stage End":
                    continue

                color = colors_dict[k]

                if k not in label_table:
                    label_table[k] = 0
                    label = k
                else:
                    label = None

                val = it[k]
                bar = ax1.bar(ind[j], val,
                                    width=width,color=color, bottom=bottom[j],
                                     ecolor='k',label=label)

                bottom[j] = bottom[j] + val

        ax1.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), fancybox=True, shadow=True, ncol=5, prop={'size': 15})
        plt.ylabel('Time(s)', fontsize=30)
        plt.xlabel(self.x_key, fontsize=30)
        plt.yticks(fontsize=30)
        ticks = sorted(items_to_plot.keys())
        plt.xticks(ind + width/2.,ticks , fontsize=30)
        plt.title("%s %s Run Time "% (self.mpi_or_spark, self.algo.upper()), fontsize=30)
        plt.show()
        return plt
    
    def set_colors_dict(self,ordered_keys):
        if self.colors_dict is None:
            assert len(ordered_keys) <= len(self.colors_list), "Too few colors specified in colors list for the number of keys to plot!"
            self.colors_dict = dict(zip(ordered_keys, self.colors_list[:len(ordered_keys)]))
        else:
            for k in ordered_keys:
                assert k in self.colors_dict, "Key %s present in data but not in specified colors_dict!" %(k)
        
    
    

class SparkPlotter(Plotter):
    def __init__(self, dict_of_pd_objects, colors_list=None, colors_dict=None, x_key='Nodes'):
        super(SparkPlotter, self).__init__(colors_list, colors_dict, 'Spark')
        self.algo = ' '.join(list(set([it.algo for it in dict_of_pd_objects.values()])))
        self.x_key = x_key
        self.dict_of_pd_objects = dict_of_pd_objects
        self.default_ordered_keys = {'start':['Driver Compute Time','Task Start Delay', 
                                              'Executor Deserialize Time', 'Scheduler Delay'], 
                                     'end':['Time Waiting Until Stage End']}
   
    def _set_up_for_plotting(self,sum_stat_fxn='mean', fold_task_overheads=True, ordered_keys=None):
        '''assume items to plot is dict of dicts, where keys are concurrency and values are dicts with runtime dist'''
        key = "Time To Finish From Stage Start"
        items_to_plot = {k: pd.get_summary_tasks_for_mpl(sum_stat_fxn, key, fold_task_overheads) 
                         for k,pd in self.dict_of_pd_objects.iteritems()}
        
        ordered_keys = self.process_ordered_keys(ordered_keys, items_to_plot)


        self.set_colors_dict(ordered_keys)
        return ordered_keys, items_to_plot
        
    def plot_runtime_distribution(self,sum_stat_fxn='mean', fold_task_overheads=True, ordered_keys=None):

        ordered_keys, items_to_plot = self._set_up_for_plotting(sum_stat_fxn, fold_task_overheads, ordered_keys)
        self._plot_bar_plot(items_to_plot, ordered_keys, self.colors_dict)
        
    def get_colors_dict(self):
        return self.colors_dict

 
    def process_ordered_keys(self, ordered_keys, items_to_plot):
        if ordered_keys is None:
            ordered_keys = self.create_ordered_keys(items_to_plot)
            [ordered_keys.remove(k) for k in self.default_ordered_keys['start'] + self.default_ordered_keys['end']]
            ordered_keys = self.default_ordered_keys['start'] + ordered_keys + self.default_ordered_keys['end']
        
        elif isinstance(ordered_keys, dict):
            '''if user specifies keys they want in start, middle or end, then we combine this ordering with
            default ordering for any of start middle and end they did not specify and then combine them along
            with what is left over'''
            
            o_keys = self.create_ordered_keys(items_to_plot)
            new_ordered_keys = []
            for order_k in ['start', 'middle', 'end']:
                if order_k not in ordered_keys:
                    ordered_keys[order_k] = self.default_ordered_keys[order_k]
            
            [o_keys.remove(key) for k in  ['start', 'middle', 'end'] for key in ordered_keys[k] ]
            ordered_keys = ordered_keys['start'] + ordered_keys['middle'] + o_keys + ordered_keys['end']
        else:
            pass #user specifies exact ordering and which keys they want so no changes are made
                    
        return ordered_keys
            
    def create_ordered_keys(self, items_to_plot):
        ordered_keys = []
        for key_l in [it.keys() for it in items_to_plot.values()]:
            ordered_keys.extend(key_l)
        ordered_keys = list(set(ordered_keys))
        return ordered_keys
        
    
class RawValuesPlotter(Plotter):
    def __init__(self, algo,x_key='Nodes', colors_list=None, colors_dict=None):
        super(RawValuesPlotter, self).__init__(colors_list, colors_dict, 'MPI')
        self.colors_dict = colors_dict
        self.algo = algo
        self.x_key = x_key
        
        
    def _set_up_for_plotting(self, items_to_plot, ordered_keys=None):
        plt.rcParams['figure.figsize'] = (20.0, 16.0)
        if ordered_keys is None:
            ordered_keys = items_to_plot.values()[0].keys()
        self.set_colors_dict(ordered_keys)
        return ordered_keys, items_to_plot
        
        
    def plot_run_time_distribution(self, items_to_plot, ordered_keys=None):
        ordered_keys = self._set_up_for_plotting(items_to_plot, ordered_keys)
        return self._plot_bar_plot(items_to_plot, ordered_keys, self.colors_dict)
    

def plot_hero(spark_plot_obj, mpi_plot_obj, mpi_times):
    ordered_keys, items_to_plot = spark_plot_obj._set_up_for_plotting()
    mpi_ordered_keys, mpi_items_to_plot = mpi_plot_obj._set_up_for_plotting(mpi_times)
    spark_times = items_to_plot.values()[0]
    mpi_times = mpi_items_to_plot.values()[0]  
    plt.rcParams['figure.figsize'] = (20.0, 16.0)
    #color_keys = color_key_dict[pd.algo]
    bottom = (len(mpi_times) + len(spark_times)) *[0]
    bars = []
    patterns = ('-', '+', 'x', '\\', '*', '.')
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ind = np.arange(len(bottom))
    width = 0.7
    label_table = {}
    j =0
    spark_mpi_inds = []
    colors_list = spark_plot_obj.colors_list
    keys = mpi_times.keys() + list(set(spark_times.keys()) - set (mpi_times.keys()))
    colors_dict = dict(zip(keys,colors_list[:len(keys)]))
    for i,k in enumerate(keys):
        if k in label_table:
            continue
        if k in mpi_times and k in spark_times:
            color = colors_dict[k]
            bar = ax1.bar(ind[j:j+2],(mpi_times[k], spark_times[k]),
                                width=width,color=color,
                                 ecolor='k',label=k)
            label_table[k] = 0
            spark_mpi_inds.extend(ind[j:j+2])
            j += 2
        elif k in spark_times:
            color = colors_dict[k]
            bar = ax1.bar(ind[j:j+1],spark_times[k],
                                width=width,color=color,
                                 ecolor='k',label=k)
            label_table[k] = 0
            j += 1


    ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
                      fancybox=True, shadow=True, ncol=3, prop={'size': 20})
    plt.ylabel('Time (log(s))', fontsize=30)
    ax1.set_yscale('log')
    plt.yticks(fontsize=30)
    ticks =np.arange(j) + width /2
    ticks[-2] -= width /2
    plt.xticks(ticks,len(spark_mpi_inds) / 2 * ['MPI', "Spark"] + ["","","","S   p   a   r   k", ""], fontsize=30)
    plt.title("%s Run Time for Hero Run "% ( 'PCA'), fontsize=30)
    plt.show()