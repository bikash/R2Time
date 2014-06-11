\name{r2t.intBittoFloat}
\alias{r2t.intBittoFloat}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
initilization function
}
\description{
initilization function
}
\usage{
r2t.intBittoFloat(i)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{i}{
%%     ~~Describe \code{i} here~~
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
function (i) 
{
    sign <- bitAnd(i, 2147483648)
    if (sign == 0) 
        sign <- 1
    else sign <- -1
    exp <- bitShiftR(bitAnd(i, 2139095040), 23)
    man <- bitAnd(i, 8388607)
    man <- bitOr(man, 8388608)
    f <- sign * man * (2^(exp - 150))
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
