

library(Rhipe)
rhinit()
map <- expression({
  msys <- function(on){
    system(sprintf("wget  %s --directory-prefix ./tmp 2> ./errors",on))
    if(length(grep("(failed)|(unable)",readLines("./errors")))>0){
      stop(paste(readLines("./errors"),collapse="\n"))
    }}

  lapply(map.values,function(x){
    x=1986+x
    on <- sprintf("http://stat-computing.org/dataexpo/2009/%s.csv.bz2",x)
    fn <- sprintf("./tmp/%s.csv.bz2",x)
    rhstatus(sprintf("Downloading %s", on))
    msys(on)
    rhstatus(sprintf("Downloaded %s", on))
    system(sprintf('bunzip2 %s',fn))
    rhstatus(sprintf("Unzipped %s", on))
    rhcounter("FILES",x,1)
    rhcounter("FILES","_ALL_",1)
  })
})
z <- rhwatch(map=map,output="/home/ekstern/haisen/bikash/tmp/airline/data", mapred=list(mapred.reduce.tasks=0),copyFiles=TRUE)


rhput("/home/ekstern/haisen/bikash/data/1987.csv", "/home/ekstern/haisen/bikash/tmp/airline/data/1987.csv")





 Year Month DayofMonth DayOfWeek DepTime CRSDepTime ArrTime CRSArrTime



library(r2time)
library(Rhipe)
rhinit()


map <- expression({


  rhcollect(d[c(1,nrow(d)),"sdepart"],d)
})
reduce <- expression(
    reduce = {
      lapply(reduce.values,function(i)
             rhcollect(reduce.key,i))}
    )
mapred <- list(mapred.reduce.tasks=0)
z <- rhwatch(map=map,reduce=reduce,setup=setup, input="/home/ekstern/haisen/bikash/tmp/airline/data/", output="/home/ekstern/haisen/bikash/tmp/airline/blocks",mapred=mapred,orderby="numeric")





## Load all the necessary libraries
library(r2time)
library(Rhipe)
rhinit()  ## Initialize rhipe framework.
library(rJava)
.jinit()    ## Initialize rJava
r2t.init()  ## Initialize R2Time  framework.
library(bitops) ## Load library for bits operation, It is used for conversion between float and integer numbers.
library(gtools)


tagk = c("host") ## Tag keys. It could be list
tagv = c("*") ## Tag values. It could be list or can be separate multiple by pipe
metric = 'r2time.load.test1' ## Assign multiple metrics
startdate ='1973/01/01-00:00:00' ## Start date and time of timeseries
enddate ='2014/06/06-07:00:00' ## End date and time of timeseries
outputdir = "/home/ekstern/haisen/bikash/tmp/airline/out/" ## Output file, should be in HDFS file system.
jobname= "Carete HDFS dataset" ## Assign relevant job description name.
mapred <- list(mapred.reduce.tasks=0) ## Mapreduce configuration, you can assign number of mapper and reducer for a task. For this case is 0, no reducer is required.

jars=c("/home/ekstern/haisen/bikash/tmp/r2time.jar","/home/ekstern/haisen/bikash/tmp/zookeeper.jar", "/home/ekstern/haisen/bikash/tmp/hbase.jar")
# This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command


## Assign Zookeeper configuration. For HBase to read data zookeeper quorum must be define.
zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='haisen24.ux.uis.no')



## running map function to caculate centroid
map <- expression({
    m <- lapply(seq_along(map.values), function(r) {
        rhcollect(map.keys[[r]],map.values[[r]])
    })
})


r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo,
      output=outputdir, jobname=jobname, mapred=mapred, map=map, reduce=0, setup=NULL)

