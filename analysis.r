#!/usr/bin/Rscript
library(data.table)
library(ggplot2)

# Read data from stdin to use this script in a command line pipeline.
data <- read.csv(file("stdin"))
# Convert the time from milliseconds to seconds.
data$timestamp=round(data$timestamp/1000)

# Do a loess fit and prediction over the whole range of seconds between the
# first and last measured value.
lo <- loess(data$value~data$timestamp, span=0.25)
minTime=min(data$timestamp)
maxTime=max(data$timestamp)
smoothedTime=seq(minTime, maxTime, 1)
smoothedValues=predict(lo, smoothedTime)

# Add boundary at 0.
smoothedValues[smoothedValues<0]=0

# Convert to date
data$timestamp <- as.POSIXct(data$timestamp, origin="1970-01-01")
smoothedTimestamp=as.POSIXct(smoothedTime, origin="1970-01-01")

# Merge the predicted and measured values. This results in a data table, 
# in which each second has a measured (if available) and predicted value.
# There is one row FOR EACH second between the first and last measurement.
dt1=data.table(data)
setkey(dt1, timestamp)
dt2=data.table(timestamp=smoothedTimestamp, valueSmoothed=smoothedValues)
setkey(dt2, timestamp)
# Take the predicted values in case no measured ones are available.
merged<-dt1[dt2,]
analysed<-merged[,list(
    timestamp=timestamp,
    value=ifelse(is.na(value), valueSmoothed, value)
)]

# Make an image of the merged data.
png("analysed.png")
qplot(analysed$timestamp, analysed$value)+xlab("Date")+ylab("Power [kW]")
dev.off()
# write the merged data to file.
write.csv(analysed, "analysed.csv")
