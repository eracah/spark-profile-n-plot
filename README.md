# spark-profile-n-plot

## Using the Notebook:
* Pull down this repo to somewhere on  Cori
* Set up a Spark iPython profile on Cori for yourself (I already did that for you, Alex)
* Copy start_notebook_from_local.sh to your local mac machine
* From your local machine:
	* ./start_notebook_from_local.sh profile=pyspark_local
*Firefox should automatically open up to a iPython notebook with your home directory on global homes
* If it does not, then open any browser and go to localhost:8181
* Now you have your Spark Notebook. Navigate to where you pulled down this repo and start a notebook for parse-eventlog.ipynb
* That is it!
