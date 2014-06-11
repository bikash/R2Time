\name{r2t.rhwatch}
\alias{r2t.rhwatch}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
initilization function
}
\description{
initilization function
}
\usage{
r2t.rhwatch(table, sdate, edate, metrics, tagk, tagv, caching = 1400L, cacheBlocks = FALSE, autoReduceDetect = FALSE, batch = 100, jars = "", zooinfo, fulltable = 0, output = "", jobname = "MapReduce job", mapred = "")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{table}{
%%     ~~Describe \code{table} here~~
}
  \item{sdate}{
%%     ~~Describe \code{sdate} here~~
}
  \item{edate}{
%%     ~~Describe \code{edate} here~~
}
  \item{metrics}{
%%     ~~Describe \code{metrics} here~~
}
  \item{tagk}{
%%     ~~Describe \code{tagk} here~~
}
  \item{tagv}{
%%     ~~Describe \code{tagv} here~~
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
  \item{batch}{
%%     ~~Describe \code{batch} here~~
}
  \item{jars}{
%%     ~~Describe \code{jars} here~~
}
  \item{zooinfo}{
%%     ~~Describe \code{zooinfo} here~~
}
  \item{fulltable}{
%%     ~~Describe \code{fulltable} here~~
}
  \item{output}{
%%     ~~Describe \code{output} here~~
}
  \item{jobname}{
%%     ~~Describe \code{jobname} here~~
}
  \item{mapred}{
%%     ~~Describe \code{mapred} here~~
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
function (table, sdate, edate, metrics, tagk, tagv, caching = 1400L, 
    cacheBlocks = FALSE, autoReduceDetect = FALSE, batch = 100, 
    jars = "", zooinfo, fulltable = 0, output = "", jobname = "MapReduce job", 
    mapred = "") 
{
    r <- r2t.getRowkeyFilter(sdate, edate, metrics, tagk, tagv)
    rows <- c(r[1], r[2])
    filter <- r[3]
    z <- rhwatch(map = map, reduce = reduce, input = r2t.hbaseinput(table = table, 
        rows = rows, caching = 1400L, cacheBlocks = FALSE, jars = jars, 
        zooinfo = zooinfo, filter = filter, batch = 100), output = output, 
        jobname = jobname, mapred = mapred, param = list(beginningOflastMonth = Sys.Date() - 
            45))
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
