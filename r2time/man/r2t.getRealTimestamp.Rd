\name{r2t.connect}
\alias{r2t.connect}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
initilization function
}
\description{
initilization function
}
\usage{
r2t.connect(ra, tablename)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{ra}{
%%     ~~Describe \code{ra} here~~
}
  \item{tablename}{
%%     ~~Describe \code{tablename} here~~
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
function (ra, tablename) 
{
    tb <- .jnew("org/apache/hadoop/hbase/client/HTable", tablename)
    j <- function(ra, tb) {
        return(list(getTagK = function() getTagK(ra = ra, tb = tb), 
            getTagV = function() getTagV(ra = ra, tb = tb), table = tb))
    }
    return(j(ra, tb))
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
