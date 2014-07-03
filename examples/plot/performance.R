##Plot caching
pdf('/home/bikash/repos/r2time/examples/plot/performance.pdf', bg = "white")



#s <- c(444, 1, 3, 10, 25)


tdp <-   c(40 ,   45,  50,  61,  81, 96 )
# tmax <-  c(688,  748, 780, 830, 875, 890)
tmean <- c(724,  776, 799, 840, 881, 896)
treg <-  c(750,  791, 820, 870, 903, 927)
tk   <-  c(1568, 1603, 1678, 1710, 1740, 1791)

plot(NULL, xlim=c(1,6), ylim=c(0,2100), xlab="Data point in millions", ylab="Elapsed time in secs",xaxt="n"  )
lines(tk, lwd=2, col="black", pch=22, lty=6)
points(tk, pch=22, cex=1, col="black", lty=6 )
axis(1, at=1:6, labels=c("25", "50", "75", "100", "125", "150"))

lines(tdp, lwd=2, col="black", type="o", pch=19, lty=3 )
points(tdp, pch=19, cex=1, col="black")

# lines(tmax, lwd=2, col="black",type="o", pch=20, lty=1)
# points(tmax, pch=23, cex=1, col="black")

lines(tmean, lwd=2, col="black",type="o", pch=25, lty=2)
points(tmean, pch=25, cex=1, col="black")

lines(treg, lwd=2, col="black",type="o", pch=24, lty=5)
points(treg, pch=24, cex=1, col="black")
grid(14, 15, lty = 2)

legend("topleft",  legend=c("K-mean", "Regression", "Mean",  "Data point" ) , cex=0.8, col=c("black","black","black","black","black"),
title="statistical functions",
pch=c(22, 24, 25, 19),               # Point type
 lty=c(6,3,1,2,5)                  # Line type
 );

dev.off()




##Plot caching
pdf('/home/bikash/repos/r2time/examples/plot/difftools.pdf', bg = "white")

s = c("25", "50", "75", "100" , "125")
y <-c( 90, 117, 131,  148, 160)  #OpenTSDB
x <-c(  40,  46,  51,   56,  61)  ##R2time
z <-c( 76, 82,  88, 92, 99)  ##HBase

legd=c("R2Time", "HBase", "OpenTSDB")

plot(NULL, xlim=c(1,5), ylim=c(0,200), xlab="Data point in millions", ylab="Elapsed time in secs",xaxt="n"  )
lines(x, lwd=2, col="black", pch=22, lty=6)
points(x, pch=22, cex=1, col="black", lty=6 )
axis(1, at=1:5, labels=c("25", "50", "75", "100" , "125"))

lines(y, lwd=2, col="black", type="o", pch=19, lty=3 )
points(y, pch=19, cex=1, col="black")

# lines(tmax, lwd=2, col="black",type="o", pch=20, lty=1)
# points(tmax, pch=23, cex=1, col="black")

lines(z, lwd=2, col="black",type="o", pch=25, lty=2)
points(z, pch=25, cex=1, col="black")

grid(14, 15, lty = 2)

legend("topleft",  legend=legd , cex=0.8, col=c("black","black","black","black"),
title="Tools",
pch=c(22, 25, 19),               # Point type
 lty=c(6,3,2)                  # Line type
 );

dev.off()


