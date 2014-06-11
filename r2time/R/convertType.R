#' Initialiazes r2time
#'

#library(rJava)
#.jinit()
#obj<-.jnew("org/r2time/DataType") #  give the fully qualified path to the class file
#sdate <-  '2013/03/12';
#edate <-  '2013/03/14';
#res<-.jcall(obj,"[j","getRowKey",sdate,edate)
#a<-r2t.getRowKey('2013/03/12','2013/03/14');

####Push element in the list############
pushdata <- function(l, x) {
  assign(l, append(eval(as.name(l)), x), envir=parent.frame())
}


pushList <- function(l) {
   lst <- get(l, parent.frame())
   a <-c("1")
   y <-append(eval(as.name("a")),lst)
   assign(l, y, envir=parent.frame())
 }


##RESULT
#### a <- list(1,2)
#### push("a", 3)
###a[[1]][1] 1[[2]][1] 2[[3]][1] 3


###Data conversion need bitops library for bit operation
r2t.toInt <-function(x)
{
	n <- 0
	for(i in 1:length(x)){
	   n <- bitShiftL(n,8)
	   n <- bitXor(n,bitAnd(x[i],0xFF))
	}
	return(n)
}

#############################IntBitToFloat conversion########################
r2t.intBittoFloat <- function(i)
{
	sign <- bitAnd(i,0x80000000)
	if(sign == 0) sign <- 1
	else sign <- -1
	exp <- bitShiftR(bitAnd(i,0x7f800000),23)
	man <- bitAnd(i,0x007fffff)
	man <- bitOr(man,0x00800000)
	f <- sign * man * (2^(exp-150))
}


###########Conversion Bytes Array to Float######################################
r2t.toFloat<- function(x){
	  if(length(x)>7){
	  	i <- r2t.toInt(c(x[1],x[2],x[3], x[4]))
	  	f<-r2t.intBittoFloat(i)
	    return(f)
	  }
	  else{
	    i<-r2t.toInt(x)
	    f<-r2t.intBittoFloat(i)
	    return(f)
        }
}
##Get Start and end rowkey with filter for tags.
r2t.getRowkeyFilter <- function(sdate,edate,metrics,tagk,tagv){
	tagk = pushList("tagk")
	if(!is.array(tagk))
		tagkey = array(data = tagk, dim = length(tagk))
	else
		tagkey =  tagk


	tagv = pushList("tagv")
	if(!is.array(tagv))
		tagvalue = array(data = tagv, dim = length(tagv))
	else
		tagvalue = tagv
	obj<-.jnew("DataType") ;


	res<-.jcall(obj,"[S","getRowkeyFilter",sdate,edate,metrics,tagkey,tagvalue);
  	#return res;
}

#r2t.getRowkeyFilter('2013/05/08 06:00:00', '2013/05/08 10:00:00','proc.loadavg.1m',list('host'),list('bikash|foo'))
#Get 4 bytes timestamp from rowkey. Assuming first 3 bytes are used for metric ID and rest 4 bytes are for timestamp.
r2t.getRowBaseTimestamp<- function(rowkey)
{
	 timestamp <- r2t.toInt(c(rowkey[[4]],rowkey[[5]],rowkey[[6]],rowkey[[7]]))
}


#Get actual timestamp by adding basetimestamp delta field.
r2t.getRealTimestamp<- function(basetimestamp,delta)
{
	a <- names(delta);
	if(!invalid(a)){
		del <- gsub("t:","",a)
		timestamp <- basetimestamp + strtoi(del)
		return(timestamp)
	}
	else
		return(0)
}


# GET base timestamp from rowkey, input as rawbytes
r2t.getBaseTimestamp1 <- function(rowkey)
{
	obj<-.jnew("DataType") ;
	res<-.jcall(obj,"I","getBaseTimestamp",rowkey);
}

# GET base timestamp from rowkey, input as rawbytes
r2t.getBaseTimestamp <- function(rowkey)
{
	# Four bits are timestamp bits.
	k<-r2t.toInt(c(rowkey[[4]],rowkey[[5]],rowkey[[6]],rowkey[[7]]))
	#return k;
}

# Sum basetime and delta value to get actual time and list of values associated with it.
r2t.getTimestamp <- function(basetime,col)
{
	lapply(seq_along(col), function(r) {
		y<-col[r]
		y.1<- attributes(y)
		y.2<-as.numeric(y.1$name)
		timestamp <- basetime + y.2
		v <- r2t.toFloat(col[[r]])
		a <- array(c(timestamp,v)) #create @D array
	})
}


##function to convert list of data into float
r2t.convertByteArraytoFloat <- function(v)
{

     obj<-.jnew("DataType") ;
     res<-.jcall(obj,"[F","convertBytetoFloat",v);
}


# Convert String Rowkey to Byte Rowkey Format
r2t.String2Bytes <- function(rowkey)
{
	obj<-.jnew("DataType") ;
	res<-.jcall(obj,"[B","String2Bytes",rowkey);
}

# Convert byte data to load
r2t.Bytes2Float <- function(data)
{
	obj<-.jnew("DataType") ;
	res<-.jcall(obj,"F","Bytes2Float",data);
}


##Get rowkey span from tsdb table
r2t.getRowKey <- function(sdate,edate,tagk,tagv,metrics){
	obj<-.jnew("DataType") ;
	res<-.jcall(obj,"[J","getRowKey",sdate,edate,tagk,tagv,metrics);
  	#return res;
}
#a<-r2t.getRowKey('2013/03/12','2013/03/14','host','pc-0-227','proc.loadavg.1m');




##Get getTimeSereiesData from tsdb table
r2t.getTimeSereiesData <- function(sdate,edate,tagk,tagv,metrics){
	obj<-.jnew("DataType") ;
	res<-.jcall(obj,"[[J","getTimeSereiesData",sdate,edate,tagk,tagv,metrics);
}


###Convert byte array to floating point
r2t.bytes2float<-function(value){
	obj<-.jnew("org.apache.hadoop.hbase.util.Bytes") ;
	res<-.jcall(obj,"F","toFloat",value);
}

# Table connection function
r2t.connect <- function(ra,tablename){
  tb <- .jnew("org/apache/hadoop/hbase/client/HTable",tablename)
	j <- function(ra,tb){
			return(list(getTagK = function()     getTagK(ra=ra,tb=tb)
						,getTagV   = function()     getTagV(ra=ra,tb=tb)
			,table   = tb))
	}
	return(j(ra,tb))
}


####Setting up HBASE INPUT FORMAT FOR RHIPE MAP REDUCE TASK
r2t.hbaseinput <- function(table, colspec=NULL, rows=NULL,caching=1000, cacheBlocks=FALSE,autoReduceDetect=FALSE ,  jars="" ,zooinfo, filter = "", batch = 1L, fulltable=0){

makeRaw <- function(a){
    #a <- if(is.character(a)) charToRaw(a)
   # if(!is.raw(a)) stop("rows must be raw")
    J("org.apache.commons.codec.binary.Base64")$encodeBase64String(.jbyte( a  ))
  }
  table <- eval(table); colspec <- eval(colspec);rows <- eval(rows);cacheBlocks <- eval(cacheBlocks)
  autoReduceDetect <- eval(autoReduceDetect)
  caching <- eval(caching)
  function(mapred,direction, callers){


      if(is.null(table)) stop("Please provide table type e.g. tsdb")
      mapred$rhipe.hbase.tablename <- as.character(table[1])
      mapred$rhipe.hbase.colspec <-NULL
      if(!is.null(rows)){
        mapred$rhipe.hbase.rowlim.start <- rows[[1]]
        mapred$rhipe.hbase.rowlim.end   <- rows[[2]]
      }
	mapred$rhipe.hbase.filter   <- filter
	mapred$rhipe.hbase.set.batch   <- batch
mapred$parse.ifolder='';
      mapred$rhipe.hbase.mozilla.cacheblocks <- sprintf("%s:%s",as.integer(caching),as.integer(cacheBlocks))
  mapred$zookeeper.znode.parent <- zooinfo$"zookeeper.znode.parent"
      mapred$hbase.zookeeper.quorum <- zooinfo$"hbase.zookeeper.quorum"
      message(sprintf("Using %s table", table))

      mapred$rhipe.hbase.dateformat <- "yyyyMMdd"
      mapred$rhipe.hbase.mozilla.prefix <-  "byteprefix"
      mapred$rhipe_inputformat_class <- 'RHHBaseRecorder'
      if(fulltable == 1){
     	 mapred$rhipe_inputformat_class <- 'RHScanTable'
	}

      mapred$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
      mapred$rhipe_inputformat_valueclass <- 'RHResult'
      mapred$jarfiles <- jars
      mapred

  }

}

###Submitting to Rhipe map reduce job
r2t.job <- function(table='tsdb',sdate,edate,metrics,tagk,tagv, caching=1400L, cacheBlocks=FALSE,autoReduceDetect=FALSE , batch=100,  jars="" ,zooinfo,  fulltable=0, output="",
	jobname="MapReduce job", mapred="",map=map,reduce=reduce, setup = NULL){

r <-r2t.getRowkeyFilter(sdate,edate,metrics,tagk,tagv)

rows <- c(r[1],r[2])
filter <- r[3]

z <- rhwatch(map=map,  reduce=reduce,
input=
r2t.hbaseinput(table=table,rows=rows, caching=1400L, cacheBlocks=FALSE, jars=jars, zooinfo=zooinfo, filter = filter,batch = 100,fulltable = fulltable)
,output = output
,jobname = jobname
,mapred = mapred
,setup = setup
,param = list(beginningOflastMonth = Sys.Date()-45)
)


}






# Initialiazes r2time
r2t.init <- function(requestAdmin=TRUE,otherConfigs=NULL,HBASE.HOME="/usr/lib/hbase",HADOOP.HOME="/usr/lib/hadoop"
                    ,HADOOP.CONF = sprintf("%s/conf",HADOOP.HOME)
                    ,HBASE.CONF = sprintf("%s/conf",HBASE.HOME)
		    ,HBASE.LIB = sprintf("%s/lib",HBASE.HOME)
                    ,rhipeJar = list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
                    ,r2timeJar = list.files(paste(system.file(package="r2time"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)){
  hadoopJars <- list.files(HADOOP.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
  hbaseJars <- list.files(HBASE.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
  hbaseLibJars <- list.files(HBASE.LIB,pattern="jar$",full.names=TRUE,rec=TRUE)
  hadoopConf <- list.files(HADOOP.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)
  hbaseConf <- list.files(HBASE.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)

  #.jinit(c(HADOOP.CONF, HBASE.CONF,rhipeJar,r2timeJar,hadoopJars,hbaseJars,hbaseLibJars))
  .jinit()
  jars <- c(HADOOP.CONF, HBASE.CONF,rhipeJar,r2timeJar,hadoopJars,hbaseJars,hbaseLibJars)
  .jaddClassPath(jars)
  if(requestAdmin){
    f <- if(!is.null(otherConfigs) && is.character(otherConfigs))
      .jnew("DataType") else .jnew("DataType")
  }else f <- NULL
  .jcheck()
  f
}


#function to get all Tag Key in tsdb-uid table
r2t.getTagKeys <- function(ra,tb){
	#obj<-.jnew("DataType") ;
	res<-.jcall(ra,"[B","getTagK",tb);
}

#function to get all Tag value in tsdb-uid table
r2t.getTagValue <- function(ra,tb,tag){
	#obj<-.jnew("DataType") ;
	res<-.jcall(ra,"[B","getTagv",tb);
}


#function to get all metrics value in tsdb-uid table
r2t.getMetrics <- function(ra,tb){
	res<-.jcall(ra,"[B","getMetrics",tb);
}


##Function to convert bytes data type to long
r2t.byte2long <- function(ra,data){
	res<-.jcall(ra,"J","byte2long",data);
}

##Function to convert long data type to bytes
r2t.long2byte <- function(ra,data){
	res<-.jcall(ra,"[B","getTagK",data);
}


##Function to conver to long data type
r2t.getLong <- function(ra,data,offset){
	res<-.jcall(ra,"J","getLong",data,offset);
}



##function to set Int value of bytes array
r2t.setInt <- function(ra,data,n){
	.jcall(ra,"V","setInt",data,n);
}



##function to set Int value of bytes array with offset value
r2t.setInt <- function(ra,data,n,offset){
	.jcall(ra,"V","setInt",data,n,offset);
}

