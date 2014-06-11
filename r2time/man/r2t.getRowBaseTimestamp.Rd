\name{r2t.getRowKey}
\alias{r2t.getRowKey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
initilization function
}
\description{
initilization function
}
\usage{
r2t.getRowBaseTimestamp(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{sdate}{
%%     ~~Describe \code{sdate} here~~
}
  \item{edate}{
%%     ~~Describe \code{edate} here~~
}
  \item{tagk}{
%%     ~~Describe \code{tagk} here~~
}
  \item{tagv}{
%%     ~~Describe \code{tagv} here~~
}
  \item{metrics}{
%%     ~~Describe \code{metrics} here~~
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
function (sdate, edate, tagk, tagv, metrics)
{
    obj <- .jnew("DataType")
    res <- .jcall(obj, "[J", "getRowKey", sdate, edate, tagk,
        tagv, metrics)
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
