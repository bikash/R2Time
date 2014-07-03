##Plot statistical file size
pdf('/home/bikash/repos/r2time/examples/plot/compare.pdf', bg = "white")

# GENERATE THE DATA

s = c("25", "50", "75", "100" , "125")

y <-c( 90, 117, 131,  148, 160)  #OpenTSDB
x <-c(  40,  46,  51,   61,  69)  ##R2time
z <-c( 39, 44,  49, 58, 67)  ##HBase



Deathrate = matrix(c(z, x, y), nrow=length(s), ncol=3, dimnames=list(s, c( "HBase","R2Time", "OpenTSDB")))
# Create a matrix Deathrate with the data
Deathrate2 = t(Deathrate)                                       # Transpose the Deathrate matrix

# SIMPLE BARPLOT

barplot(Deathrate2,                             # Data (bar heights) to plot
        beside=TRUE,                            # Plot the bars beside one another; default is to plot stacked bars
        names.arg=s,
        col=c( "white", "black", "gray"),                   # Color of the bars
        border="black",                         # Color of the bar borders
        #main=c("Performance with different type of input format"),      # Main title for the plot
        xlab="Data point in millions",                       # X-axis label
        ylab="Elapsed time in secs",                      # Y-axis label
        font.lab=3)                             # Font to use for the axis labels: 1=plain text, 2=bold, 3=italic, 4=bold italic

legend("topleft",                               # Add a legend to the plot
        legend=c( "HBase", "R2Time", "OpenTSDB"),             # Text for the legend
        fill=c( "white","black",  "gray"))                  # Fill for boxes of the legend


dev.off()



##Plot statistical file size
pdf('/home/bikash/repos/r2time/examples/plot/node.pdf', bg = "white")

# GENERATE THE DATA
s = c("2", "4", "6")
x <-c(  77,  56,  49)  ##R2time
y <-c( 149, 141, 133)  #<OpenTSDB
z <-c( 75, 55, 48)  ##HBase
Deathrate = matrix(c(z, x, y), nrow=length(s), ncol=3, dimnames=list(s, c( "HBase", "R2Time", "OpenTSDB")))
# Create a matrix Deathrate with the data
Deathrate2 = t(Deathrate)                                       # Transpose the Deathrate matrix

# SIMPLE BARPLOT
barplot(Deathrate2,                             # Data (bar heights) to plot
        beside=TRUE,                            # Plot the bars beside one another; default is to plot stacked bars
        names.arg=s,
        col=c("white", "black",  "gray"),                   # Color of the bars
        border="black",                         # Color of the bar borders
        #main=c("Performance with different type of input format"),      # Main title for the plot
        xlab="number of nodes",                       # X-axis label
        ylab="Elapsed time in secs",                      # Y-axis label
        font.lab=3)                             # Font to use for the axis labels: 1=plain text, 2=bold, 3=italic, 4=bold italic

legend("topleft",                               # Add a legend to the plot
        legend=c( "HBase", "R2Time", "OpenTSDB"),             # Text for the legend
        fill=c( "white", "black", "gray"))                  # Fill for boxes of the legend

dev.off()



##Plot statistical file size with node 3 down
pdf('/home/bikash/repos/r2time/examples/plot/nodedown3.pdf', bg = "white")

# GENERATE THE DATA
s = c("1", "2", "3", "4")
x <-c(  49,  51,  51,  50)  ##R2time
x.1 <-c(  49,  51,  59,  51)  ##R2time with node 2 down
y <-c( 133, 131, 131, 133)  #<OpenTSDB
Deathrate = matrix(c(x, x.1, y), nrow=length(s), ncol=4, dimnames=list(s, c("R2Time","R2Time with node down in 3rd iteration", "OpenTSDB")))
# Create a matrix Deathrate with the data
Deathrate2 = t(Deathrate)                                       # Transpose the Deathrate matrix

# SIMPLE BARPLOT
barplot(Deathrate2,                             # Data (bar heights) to plot
        beside=TRUE,                            # Plot the bars beside one another; default is to plot stacked bars
        names.arg=s,
        col=c("black", "black", "black"),                   # Color of the bars
        border="black",                         # Color of the bar borders
        #main=c("Performance with different type of input format"),      # Main title for the plot
        xlab="iterations",                       # X-axis label
        ylab="Elapsed time in secs",                      # Y-axis label
        font.lab=3,
        density = c(24,24,24), angle = c(45, 180, -45) )                             # Font to use for the axis labels: 1=plain text, 2=bold, 3=italic, 4=bold italic

legend("topleft",                               # Add a legend to the plot
        legend=c("R2Time", "R2Time with node down in 3rd iteration", "OpenTSDB"),             # Text for the legend
        fill=c("black", "black", "black"), density = c(24,24,24), angle = c(45,  180, -45))                  # Fill for boxes of the legend

dev.off()