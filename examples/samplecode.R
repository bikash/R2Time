
# Load all the necessary libraries
library(r2time)
library(Rhipe)
rhinit()  ## Initialize rhipe framework.
r2t.init()  ## Initialize R2Time  framework.

map <- expression({
    g.max <- lapply(seq_along(map.values), function(r) {
			v <- r2t.toInt(map.values[[r]])
     })
	rhcollect(1,max(unlist(g.max)))
})

reduce <- expression(
   pre={
      global.max <- NULL
   },
   reduce={
      global.max <-  max(global.max, unlist(reduce.values))
   },
   post={
      rhcollect(reduce.key, global.max)
   }
)
# run map reduce job.
r2t.job(startdate,enddate, metric, tagkeys, tagvalues, zooinfo,  output, map, reduce)

