#########################################################################################
# Author: Bikash Agrawal
# Date: 28-06-2014
# Description: This example is used to calculate  Kmean clustering
# source("/home/bikash/repos/r2time/examples/Kmean.R")
#########################################################################################

## Load all the necessary libraries
library(r2time)
library(Rhipe)
rhinit()	## Initialize rhipe framework.
library(rJava)
.jinit()    ## Initialize rJava
r2t.init()  ## Initialize R2Time  framework.
library(bitops) ## Load library for bits operation, It is used for conversion between float and integer numbers.
library(gtools)


tagk = c("host") ## Tag keys. It could be list
tagv = c("*")	## Tag values. It could be list or can be separate multiple by pipe
metric = 'r2time.stress.test' ## Assign multiple metrics
startdate ='1980/01/19-00:00:00' ## Start date and time of timeseries
enddate ='2014/09/20-04:00:00' ## End date and time of timeseries
output = "/home/bikash/tmp/ex1.1" ## Output file, should be in HDFS file system.
jobname= "Kmean clustering example 1.1" ## Assign relevant job description name.
mapred <- list(mapred.reduce.tasks=0) ## Mapreduce configuration, you can assign number of mapper and reducer for a task. For this case is 0, no reducer is required.

# rhput("/home/ekstern/haisen/bikash/jar/r2time.jar", "/home/ekstern/haisen/bikash/tmp/r2time.jar")
# rhput("/home/ekstern/haisen/bikash/asynchbase.jar", "/home/ekstern/haisen/bikash/tmp/asynchbase.jar")
# rhput("/home/ekstern/haisen/bikash/hbase.jar" , "/home/ekstern/haisen/bikash/tmp/hbase.jar")
# rhput("/home/ekstern/haisen/bikash/zookeeper.jar" , "/home/ekstern/haisen/bikash/tmp/zookeeper.jar")
jars=c("/home/ekstern/haisen/bikash/jar/r2time.jar","/home/ekstern/haisen/bikash/tmp/zookeeper.jar", "/home/ekstern/haisen/bikash/tmp/hbase.jar", "/home/bikash/tmp/asynchbase.jar")
# This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command


## Assign Zookeeper configuration. For HBase to read data zookeeper quorum must be define.
zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='haisen24.ux.uis.no')



outputdir <- "/home/ekstern/haisen/bikash/tmp/out01"
## running map function to caculate centroid
map <- expression({
    library(bitops)
    library(r2time)
    library(gtools)
    m <- lapply(seq_along(map.values), function(r) {
        v <- r2t.toFloat(map.values[[r]][[1]])
        k1<-r2t.getRowBaseTimestamp(map.keys[[r]])
        k <-r2t.getRealTimestamp(k1,map.values[[r]])
        rhcollect(k,v)
    })
 })

## Run job in R2Time.
r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo,
        output=outputdir, jobname=jobname, mapred=mapred, map=map, reduce=0, setup=NULL)


#########################################################################################


library(plyr)
library(Rhipe)
rhinit()

bySpecies <- lapply(unique(iris$Species), function(x) {
    list(as.character(x), subset(iris, Species==x))
})

rhwrite(bySpecies, "/tmp/userhipe/bySpecies")

rhls("/tmp/userhipe/bySpecies/")
tmp <- rhread("/tmp/userhipe/bySpecies")

##mapper
map <- expression({
   lapply(seq_along(map.values), function(r) {
      v <- map.values[[r]]
      k <- map.keys[[r]]
      rhcollect(k, mean(v$Petal.Length))
   })
})


z <- rhwatch(map=map,  reduce=0,
input= "/tmp/userhipe/bySpecies/*"
,output = "/tmp/userhipe/ex1.1"
,jobname = "TEst Rhipe functionality"
,mapred = list(mapred.reduce.tasks=0)
,param = list(beginningOflastMonth = Sys.Date()-45)
)


