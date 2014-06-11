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

#rhput("/home/bikash/jar/r2time.jar", "/home/bikash/tmp/r2time.jar")
#rhput("/home/bikash/jar/asynchbase.jar", "/home/bikash/tmp/asynchbase.jar")
# rhput("/home/bikash/jar/hbase.jar" , "/home/bikash/tmp/hbase.jar")
# rhput("/home/bikash/jar/zookeeper.jar" , "/home/bikash/tmp/zookeeper.jar")
jars=c("/home/bikash/tmp/r2time.jar","/home/bikash/tmp/zookeeper.jar", "/home/bikash/tmp/hbase.jar", "/home/bikash/tmp/asynchbase.jar")
# This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command


## Assign Zookeeper configuration. For HBase to read data zookeeper quorum must be define.
zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='localhost')

map.setup = expression({
    x <- load("/home/bikash/tmp/centers.Rdata") # no need to give full path
    c1 <- get(x[1])
    c2 <- get(x[2])
 })

## running map function to caculate centroid
map <- expression({
    library(bitops)
    library(r2time)
    library(gtools)
	m <- lapply(seq_along(map.values), function(r) {
        v <- r2t.toFloat(map.values[[r]][[1]])
        k1<-r2t.getRowBaseTimestamp(map.keys[[r]])
        k <-r2t.getRealTimestamp(k1,map.values[[r]])
        a <- list(k,v)
	})
    y = matrix(unlist(m), ncol=2, byrow=TRUE)
	if(nrow(y) > 0)
	{
		col.x = y[,1]
		col.y = y[,2]
		c1<-c1
		c2<-c2
		centerMat<-rbind(c1,c2)
		#forming the full data frame
		d<-data.frame(col.x=col.x, col.y=col.y, stringsAsFactors=FALSE)
		#Appeding the center matrix to the top of the data frame
		dmat<-rbind(centerMat,as.matrix(d))
		#Finding the euclidean distance
		reqMat<-(as.matrix(dist(dmat,method="euclidean")))[4:nrow(y),1:2]
		#creating three data frame for three different centers
		d1<-data.frame()#data frame for centre1
		d2<-data.frame()#data frame for centre2
		for( i in 1:nrow(reqMat) )
	 	{
	    	minimum = which.min(reqMat[i,])
	    	if(minimum==1)      d1<-rbind(d1,d[i, ])
	    	else if(minimum==2) d2<-rbind(d2,d[i, ])
		}
		rhcollect(c1,d1)
		rhcollect(c2,d2)
	}
 })

# Reduce function to calculate to final centroid.
# reduce<-expression(
# 	pre = { collect<-NULL } ,
# 	reduce = {
#             collect<-rbind(collect,do.call("rbind",reduce.values))
#             },
#     post = {
#             rhcollect(reduce.key,collect)
#            }
#  )


## create inital centroids with k =2 choosen randomly
c1 <-c(1392739733,5.47)
c2 <-c(1392734900,1.68)

#writing the centers to a file
write("Initial Centers",file="/home/bikash/tmp/centers",append=TRUE)
write(c1,file="/home/bikash/tmp/centers",append=TRUE)
write(c2,file="/home/bikash/tmp/centers",append=TRUE)
write("---------------------------------------------",file="/home/bikash/tmp/centers",append=TRUE)
#rhsave(c1,c2,file="/home/bikash/tmp/centers.Rdata")
save(c1,c2,file="/home/bikash/tmp/centers.Rdata")

outputdir <- "/home/bikash/tmp/out01"
breakFlag <- FALSE
for( j in 1:6)
{
	if(!breakFlag)
    {
        if( j > 1)
        	rhdel("/home/bikash/tmp/out01") #delete after you run mapreduce.
        ## Run job in R2Time.
    	r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo,
    	output=outputdir, jobname=jobname, mapred=mapred, map=map, reduce=0, setup=map.setup)
        ## Read Output file
        output<-rhread("/home/bikash/tmp/out01/part-m-00000")
        centers<-list(output[[1]][[1]],output[[2]][[1]])
        # center1<-c(mean(output[[1]][[2]]$col.x),mean(output[[1]][[2]]$col.y))
        # center2<-c(mean(output[[2]][[2]]$col.x),mean(output[[2]][[2]]$col.y))
        center1<-c(mean(output[[1]][[2]]$col.x),mean(output[[1]][[2]]$col.y))
        center2<-c(mean(output[[2]][[2]]$col.x),mean(output[[2]][[2]]$col.y))
        cat(c1)
        cat(c2)

      	for(i in 1:length(centers))
        {
            if((identical(center1,c1)) && (identical(center2, c2)))
            {
                breakFlag <- TRUE
                break
            }
            if(identical(centers[[i]],c1))
                c1 = center1
            else if(identical(centers[[i]], c2))
                c2 = center2
        }
        #writing the new centers to a file
        write(paste("Center After iteration",j,sep=":"),file="/home/bikash/tmp/centers",append=TRUE)
        write(c1,file="/home/bikash/tmp/centers",append=TRUE)
        write(c2,file="/home/bikash/tmp/centers",append=TRUE)
        write("---------------------------------------------",file="/home/bikash/tmp/centers",append=TRUE)
        #deleting the previous centers and output
        #rhdel("/home/bikash/tmp/centers.Rdata")
        fn <- "/home/bikash/tmp/centers.Rdata"
        if (file.exists(fn)) file.remove(fn)
        #saving the new centers
        #rhsave(c1,c2,file="/home/bikash/tmp/centers.Rdata")
        save(c1,c2,file="/home/bikash/tmp/centers.Rdata")
        # df <- data.frame(c1,c2)
        # write.table(df,'centers', col.names=TRUE)
    }
}

#########################################################################################