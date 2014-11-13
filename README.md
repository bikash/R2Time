R2Time connector for R to Hbase for time-series data stored by OpenTSDB.  


#Prerequisite Installation for R2Time.
1. RHIPE
Installation steps are mention in the following link:
https://www.datadr.org/install.html

2. OpenTSDB
Installation steps are mention in the following link:
http://opentsdb.net/docs/build/html/installation.html#id1

Check OpenTSDB is runing successfully.
IP:4242

#Installation of R2Time.
```
1. Download R2Time https://github.com/bikash/R2Time/releases/tag/V.1
2. $ R CMD INSTALL r2time_1.0.tar.gz
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

R2time.jar can be download from GitHUB

#Example:
Now running simple count example in R2Time.

```
Load all the necessary libraries
1. library(r2time)
2. library(Rhipe)
3. rhinit()	## Initialize rhipe framework.
4. library(rJava)
5. .jinit()    ## Initialize rJava
6. r2t.init()  ## Initialize R2Time  framework.
7. library(bitops) ## Load library for bits operation, It is used for conversion between float and integer numbers.
8. library(gtools)


9. tagk = c("host") ## Tag keys. It could be list
10. tagv = c("*")	## Tag values. It could be list or can be separate multiple by pipe
11. metric = 'r2time.load.test1' ## Assign multiple metrics
12. startdate ='2000/01/01-00:00:00' ## Start datetime of timeseries
13. enddate ="2003/01/31-10:00:00"   ## END datetime of timeseries
14. outputdir = "/home/bikash/tmp/mean/ex1.1" ## Output file location , should be in HDFS file system.
15. jobname= "Calculation for number of DP for 75 million Data points with 2 node" ## Assign relevant job description name.
16. mapred <- list(mapred.reduce.tasks=0) ## Mapreduce configuration, you can assign number of mapper and reducer for a task. For this case is 0, no reducer is required.
17. #Location of jar file in HDFS file system. Replace "/home/ekstern/haisen/bikash/tmp/r2time.jar" with your required hdfs_location of jar files.
18. jars=c("/home/ekstern/haisen/bikash/tmp/r2time.jar","/home/ekstern/haisen/bikash/tmp/zookeeper.jar", "/home/ekstern/haisen/bikash/tmp/hbase.jar")
19. # This jars need to be in HDFS file system. You can copy jar in HDFS using RHIPE rhput command
 
21. ## Assign Zookeeper configuration. For HBase to read data zookeeper quorum must be define.
22. zooinfo=list(zookeeper.znode.parent='/hbase',hbase.zookeeper.quorum='haisen24.ux.uis.no')
 
24. ## running map function to caculate centroid
25. map <- expression({
	26. library(bitops)
	27. library(r2time)
	28. library(gtools)
	29. len <- 0
	30. m <- lapply(seq_along(map.values), function(r) {
	31. 	attr <- names(map.values[[r]]);
	32. 	leng <-  length(attr)
	33. 	})
	34. 	rhcollect(1,sum(unlist(m)))
35. })

36. #Reduce function to calculate to final centroid.
37. reduce <- expression(
	38. pre={ len <- 0 }, 
	39. reduce={ len <- len + sum(sapply(reduce.values, function(x) sum(x))) }
	40. post={ rhcollect(reduce.key, len)
41. })
 
43. ## Run job.
44. r2t.job(table='tsdb',sdate=startdate, edate=enddate, metrics=metric, tagk=tagk, tagv=tagv, jars=jars, zooinfo=zooinfo,	output=outputdir, jobname=jobname, mapred=mapred, map=map, reduce=reduce, setup=NULL)
45. t = rhread(outputdir)
```



