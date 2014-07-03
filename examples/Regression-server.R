#########################################################################################
# Author: Bikash Agrawal
# Date: 28-06-2014
# Description: This example is used to calculate  mean normal read and write operation in hbase
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
metric = 'r2time.load.test1' ## Assign multiple metrics
startdate ='1973/01/01-00:00:00' ## Start date and time of timeseries
#enddate ='2002/01/31-07:00:00' ## End date and time of timeseries for 50
#enddate ='2000/11/17-10:00:00' ## End date and time of timeseries for 25 m ds
#enddate ='2003/03/17-10:00:00' ## End date and time of timeseries for 75 m  ds
#startdate = "2004/01/01-00:00:00"
enddate ="2014/07/28-10:00:00"

outputdir = "/home/bikash/tmp/mean/ex1.1" ## Output file, should be in HDFS file system.
jobname= "Calculation of Regression for 150 million Data points" ## Assign relevant job description name.
mapred <- list(mapred.reduce.tasks=1) ## Mapreduce configuration, you can assign number of mapper and reducer for a task. For this case is 0, no reducer is required.


jars=c("/home/ekstern/haisen/bikash/tmp/r2time.jar","/home/ekstern/haisen/bikash/tmp/zookeeper.jar", "/home/ekstern/haisen/bikash/tmp/hbase.jar")
# This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command

## Assign Zookeeper configuration. For HBase to read data zookeeper quorum must be define.
zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='haisen24.ux.uis.no')



## map function to calculate X*X and X*Y and summation
map <- expression({
    library(bitops)
    library(r2time)
    library(gtools)
    m <- lapply(seq_along(map.values), function(r) {
        attr <- names(map.values[[r]]);
        val  <- map.values[[r]]
        k1<-r2t.getRowBaseTimestamp(map.keys[[r]])
        v <- lapply(seq_along(attr), function(l) {
            v <- r2t.toInt(val[[l]])
        })
    })
    k.1 <- lapply(seq_along(map.values), function(r) {
        k1<- r2t.getRowBaseTimestamp(map.keys[[r]])
        k <- r2t.getRealTimestamp(k1,map.values[[r]])
    })

    mf1 = matrix(unlist(m), ncol=1, byrow=TRUE)
    mm = matrix(unlist(k.1), ncol=1, byrow=TRUE)
    xpx <-  crossprod(mm) ## (t(mm) %*% mm) , see "Least Squares
                              ## Calculations in R", by Douglas Bates, R News,
                              ## 2004 - http://cran.r-project.org/doc/Rnews/Rnews_2004-1.pdf
    xpy <-  t(crossprod(mm, mf1)) ## t(t(mm) %*% mf1)
    ypy <- sum(mf1 * mf1)    # sum of y^2
    ys <- sum(mf1)       # sum of y
    rhcollect(0L,xpx)
    rhcollect(1L,xpy)
    rhcollect(2L,as.numeric(c(ys,ypy)))
})

reduce <- expression(
      pre={sums <- 0;} ,
      reduce = {
        if(reduce.key[[1]]==2L) sums <- sums+apply(do.call("rbind",reduce.values),2,sum)
        else for(i in reduce.values) sums <- sums+i
      },
      post = {
        rhcollect(reduce.key, sums)
      }
)


r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo,
    	output=outputdir, jobname=jobname, mapred=mapred, map=map, reduce=reduce, setup=NULL)

t = rhread(outputdir)
