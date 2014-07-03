
tagk = c("host") ## Tag keys. It could be list
tagv = c("*")	## Tag values. It could be list or can be separate multiple by pipe
metric = 'r2time.load.test1' ## Assign multiple metrics
startdate ='1973/01/01-00:00:00' ## Start date and time of timeseries
#enddate ='2002/01/31-07:00:00' ## End date and time of timeseries for 50
#enddate ='2000/11/17-10:00:00' ## End date and time of timeseries for 25 m ds
#enddate ='2003/03/17-10:00:00' ## End date and time of timeseries for 75 m  ds
#startdate = "2004/01/01-00:00:00"
enddate ="2014/07/28-10:00:00"

outputdir = "/home/bikash/tmp/mean/ex1.1" ## Output file, should be in HDFS file system.
jobname= "Calculation for roww-counter for 150 million Data points" ## Assign relevant job description name.
mapred <- list(mapred.reduce.tasks=1) ## Mapreduce configuration, you can assign number of mapper and reducer for a task. For this case is 0, no reducer is required.


jars=c("/home/ekstern/haisen/bikash/tmp/r2time.jar","/home/ekstern/haisen/bikash/tmp/zookeeper.jar", "/home/ekstern/haisen/bikash/tmp/hbase.jar")
# This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command

## Assign Zookeeper configuration. For HBase to read data zookeeper quorum must be define.
zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='haisen24.ux.uis.no')


## running map function to caculate centroid
map <- expression({
    library(gtools)
    rhcollect(1,length(map.values))
 })

#Reduce function to calculate to final centroid.
reduce <- expression(
   pre={
      len <- 0
   },
   reduce={
      len <- len + sum(sapply(reduce.values, function(x) sum(x)))
   },
   post={
      rhcollect(reduce.key, len)
})

r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo,
    	output=outputdir, jobname=jobname, mapred=mapred, map=map, reduce=reduce, setup=NULL)

t = rhread(outputdir)