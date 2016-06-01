# spark-profile-n-plot

## Using the Notebook:
* Pull down this repo to somewhere on your home directory (on Cori or Edison, etc.)
* Set up a Spark iPython profile on your home direcotry for yourself (I already did that for you, Alex)
* Add "module load python" to your .bashrc.ext file in your home directory
* Copy start_notebook_from_local.sh to your local mac machine
* From your local machine:
	* ./start_notebook_from_local.sh --profile=pyspark_local
* Firefox should automatically open up to an iPython notebook with your home directory on global homes
* If it does not, then open any browser and go to localhost:8181
* Do not close the shell where you called start_notebook_local.sh from
* Now you have your Spark Notebook. Navigate to where you pulled down this repo and start a notebook for parse-eventlog.ipynb
* That is it!
