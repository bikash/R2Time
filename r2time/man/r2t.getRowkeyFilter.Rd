\name{r2t.getRowkeyFilter}
\alias{r2t.getRowkeyFilter}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
initilization function
}
\description{
initilization function
}
\usage{
r2t.getRowkeyFilter(sdate, edate, metrics, tagk, tagv)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
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
function (sdate, edate, metrics, tagk, tagv) 
{
    tagk = pushList("tagk")
    if (!is.array(tagk)) 
        tagkey = array(data = tagk, dim = length(tagk))
    else tagkey = tagk
    tagv = pushList("tagv")
    if (!is.array(tagv)) 
        tagvalue = array(data = tagv, dim = length(tagv))
    else tagvalue = tagv
    obj <- .jnew("DataType")
    res <- .jcall(obj, "[S", "getRowkeyFilter", sdate, edate, 
        metrics, tagkey, tagvalue)
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
