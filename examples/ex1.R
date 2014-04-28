#########################################################################################
# Author: Bikash Agrawal
# Date: 06-07-2013
# Description: This example is used to calculate mean running MapReduce for timeseries data
#stored by OpenTSDB
#########################################################################################

## Load all the necessary libraries
library(r2time) 
library(Rhipe)
rhinit()	## Initialize rhipe framework.
library(rJava) 
.jinit()    ## Initialize rJava
r2t.init()  ## Initialize R2Time  framework.
library(bitops) ## Load library for bits operation, It is used for conversion between float and integer numbers.

tagk = c("host") ## Tag keys. It could be list
tagv = c("*")	## Tag values. It could be list or can be separate multiple by pipe
metric = 'r2time.load.test' ## Assign multiple metrics
startdate ='2012/01/19-00:00:00' ## Start date and time of timeseries
enddate ='2014/01/19-04:00:00' ## End date and time of timeseries
output = "/home/bikash/tmp/ex1.1" ## Output file, should be in HDFS file system.
jobname= "MapReduce job example 1.1" ## Assign relevant job description name.
mapred <- list(mapred.reduce.tasks=0) ## Mapreduce configuration, you can assign number of mapper and reducer for a task. For this case is 0, no reducer is required.

rhput("/home/bikash/jar/r2time.jar", "/home/bikash/tmp/r2time.jar")
#rhput("/home/bikash/jar/asynchbase.jar", "/home/bikash/tmp/asynchbase.jar")

jars=c("/home/bikash/tmp/r2time.jar","/home/bikash/tmp/zookeeper.jar", "/home/bikash/tmp/hbase.jar", "/home/bikash/tmp/asynchbase.jar")
# This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command
# rhput("/home/bikash/jar/r2time.jar", "/home/bikash/tmp/r2time.jar")
# rhput("/home/bikash/jar/hbase.jar" , "/home/bikash/tmp/hbase.jar")
# rhput("/home/bikash/jar/zookeeper.jar" , "/home/bikash/tmp/zookeeper.jar")

## Assign Zookeeper configuration. For HBase to read data zookeeper quorum must be define.
zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='localhost')

## Map function to calculate mean. At first it will convert all byte array in to float point, then it will calculate mean rowwise, Then finally overall mean.
map <- expression({
    library(bitops)
    library(r2time)
    lapply(seq_along(map.values), function(r) {
        v <- r2t.toFloat(map.values[[r]][[1]])
        k<-r2t.getRowBaseTimestamp(map.keys[[r]])
        rhcollect(k,map.values[[r]])
		#rhcollect(k,v)
	})
})

## Run job in R2Time.
r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo, 
	output=output, jobname=jobname, mapred=mapred, map=map, reduce=0)

## Read Output file 
out <- rhread(output)
out

#########################################################################################