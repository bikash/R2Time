\name{r2t.hbaseinput}
\alias{r2t.hbaseinput}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
initilization function
}
\description{
initilization function
}
\usage{
r2t.hbaseinput(table, colspec = NULL, rows = NULL, caching = 1000, cacheBlocks = FALSE, autoReduceDetect = FALSE, jars = "", zooinfo, filter = "", batch = 1L, fulltable = 0)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{table}{
%%     ~~Describe \code{table} here~~
}
  \item{colspec}{
%%     ~~Describe \code{colspec} here~~
}
  \item{rows}{
%%     ~~Describe \code{rows} here~~
}
  \item{caching}{
%%     ~~Describe \code{caching} here~~
}
  \item{cacheBlocks}{
%%     ~~Describe \code{cacheBlocks} here~~
}
  \item{autoReduceDetect}{
%%     ~~Describe \code{autoReduceDetect} here~~
}
  \item{jars}{
%%     ~~Describe \code{jars} here~~
}
  \item{zooinfo}{
%%     ~~Describe \code{zooinfo} here~~
}
  \item{filter}{
%%     ~~Describe \code{filter} here~~
}
  \item{batch}{
%%     ~~Describe \code{batch} here~~
}
  \item{fulltable}{
%%     ~~Describe \code{fulltable} here~~
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (table, colspec = NULL, rows = NULL, caching = 1000, 
    cacheBlocks = FALSE, autoReduceDetect = FALSE, jars = "", 
    zooinfo, filter = "", batch = 1L, fulltable = 0) 
{
    makeRaw <- function(a) {
        J("org.apache.commons.codec.binary.Base64")$encodeBase64String(.jbyte(a))
    }
    table <- eval(table)
    colspec <- eval(colspec)
    rows <- eval(rows)
    cacheBlocks <- eval(cacheBlocks)
    autoReduceDetect <- eval(autoReduceDetect)
    caching <- eval(caching)
    function(mapred, direction, callers) {
        if (is.null(table)) 
            stop("Please provide table type e.g. tsdb")
        mapred$rhipe.hbase.tablename <- as.character(table[1])
        mapred$rhipe.hbase.colspec <- NULL
        if (!is.null(rows)) {
            mapred$rhipe.hbase.rowlim.start <- rows[[1]]
            mapred$rhipe.hbase.rowlim.end <- rows[[2]]
        }
        mapred$rhipe.hbase.filter <- filter
        mapred$rhipe.hbase.set.batch <- batch
        mapred$parse.ifolder = ""
        mapred$rhipe.hbase.mozilla.cacheblocks <- sprintf("\%s:\%s", 
            as.integer(caching), as.integer(cacheBlocks))
        mapred$zookeeper.znode.parent <- zooinfo$zookeeper.znode.parent
        mapred$hbase.zookeeper.quorum <- zooinfo$hbase.zookeeper.quorum
        message(sprintf("Using \%s table", table))
        mapred$rhipe.hbase.dateformat <- "yyyyMMdd"
        mapred$rhipe.hbase.mozilla.prefix <- "byteprefix"
        mapred$rhipe_inputformat_class <- "RHScanTable"
        mapred$rhipe_inputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
        mapred$rhipe_inputformat_valueclass <- "RHResult"
        mapred$jarfiles <- jars
        mapred
    }
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
