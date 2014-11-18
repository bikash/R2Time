map <- expression({
    library(bitops)
    library(r2time)
    library(gtools)
    m <- lapply(seq_along(map.values), function(r) {
        v <- r2t.toInt(map.values[[r]][[1]])
        k1<-r2t.getRowBaseTimestamp(map.keys[[r]])
        key <-r2t.getRealTimestamp(k1,map.values[[r]])
        rhcollect(1,v)
    })
})


