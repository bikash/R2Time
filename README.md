R2Time connector for R to Hbase for time-series data stored by OpenTSDB.  
Analyzing time-series datasets at a massive scale is one of the biggest challenges that data scientists are facing.
 This implementation of a tool is used for analyzing large time-series data.
 It describes a way to analyze the data stored by OpenTSDB. OpenTSDB is an open
 source distributed and scalable time series database. 
 Currently tools available for time-series analysis are time and memory consuming.
 Moreover, no single tool exists that specializes on providing an efficient implementations
 of analyzing time-series data through MapReduce programming model at massive
 scale. For these reason, we have designed an efficient and distributed computing
 framework - R2Time. 
 
###Implementation for R2Time:###
```
Master Thesis: 
http://brage.bibsys.no/xmlui/handle/11250/181819

Published Paper CloudCom 2014:
http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=7037792
```

##Prerequisite Installation for R2Time.##
1. RHIPE
Installation steps are mention in the following link:
https://www.datadr.org/install.html

2. OpenTSDB
Installation steps are mention in the following link:
http://opentsdb.net/docs/build/html/installation.html#id1

Check OpenTSDB is runing successfully.
```
http://127.0.0.1:4242
```

##Installation of R2Time.##
```
1. $ git clone https://github.com/bikash/R2Time.git
2. $ cd R2Time
3. $ R CMD INSTALL r2time_1.0.tar.gz
```

To run R2Time it is necessary to have r2time.jar, hbase.jar, zookeeper.jar, asynchbase.jar to your HDFS location. Using rhput command from RHIPE, we can copy to HDFS location.
```
$ R
> library(RHIPE)
> rhinit()
> rhput("src_location_hbase_jar", "hdfs_location")
> rhput("src_location_zookeeper_jar", "hdfs_location")
> rhput("src_location_r2time_jar", "hdfs_location")
> rhput("src_location_asynchbase_jar", "hdfs_location")
```

R2time.jar can be downloaded from GitHUB

###Example:###
Now running simple count example in R2Time.

```
#Load all the necessary libraries
library(r2time)
library(Rhipe)
rhinit()	## Initialize rhipe framework.
library(rJava)
r2t.init()  ## Initialize R2Time  framework.
library(bitops) ## Load library for bits operation, It is used for conversion between float and integer numbers.
library(gtools)


tagk = c("host") ## Tag keys. It could be list
tagv = c("*")	## Tag values. It could be list or can be separate multiple by pipe
metric = 'r2time.load.test1' ## Assign multiple metrics
startdate ='2000/01/01-00:00:00' ## Start datetime of timeseries
enddate ="2003/01/31-10:00:00"   ## END datetime of timeseries
outputdir = "/home/bikash/tmp/mean/ex1.1" ## Output file location , should be in HDFS file system.
jobname= "Calculation for number of DP for 75 million Data points with 2 node" ## Assign relevant job description name.
mapred <- list(mapred.reduce.tasks=0) ## Mapreduce configuration, you can assign number of mapper and reducer for a task. For this case is 0, no reducer is required.
#Location of jar file in HDFS file system. Replace "/home/ekstern/haisen/bikash/tmp/r2time.jar" with your required hdfs_location of jar files.
jars=c("/home/ekstern/haisen/bikash/tmp/r2time.jar","/home/ekstern/haisen/bikash/tmp/zookeeper.jar", "/home/ekstern/haisen/bikash/tmp/hbase.jar")
# This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command
 
## Assign Zookeeper configuration. For HBase to read data, zookeeper quorum must be define.
zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='haisen24.ux.uis.no')
 
## running map function
map <- expression({
	library(bitops)
	library(r2time)
	library(gtools)
	len <- 0
	m <- lapply(seq_along(map.values), function(r) {
	 	attr <- names(map.values[[r]]);
		leng <-  length(attr)
		})
	 	rhcollect(1,sum(unlist(m)))
 })

#Reduce function 
reduce <- expression(
	pre={ len <- 0 }, 
	reduce={ len <- len + sum(sapply(reduce.values, function(x) sum(x))) }
	post={ rhcollect(reduce.key, len)
})
 
## Run job.
r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo,	output=outputdir, jobname=jobname, mapred=mapred, map=map, reduce=reduce, setup=NULL)
t = rhread(outputdir)
```


### LIST OF FUNCTIONS ###

r2t.job:
Submit job to MapReduce. 
Input Parameters:
```
table = name of the table by default 'tsdb'
sdate = Start data of metric
edate = End date of Metric
Metrics= Name of metric
tagk = List of tag keys
tagv = List of tag values
jars = list of jars files in HDFS location
output = Path of output result to be stored in HDFS.
zooinfo = Zookeeper informations
jobname = Name of job by default MapReduce Job
map = name of map function
reduce = name of reduce function, if no reduce assign 0
setup = initialization function need before map function. 
```
