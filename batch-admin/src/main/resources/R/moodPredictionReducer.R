#! /usr/bin/env Rscript
#
# One of the disadvantages of using Streaming directly is that while the inputs to the reducer are grouped by key,
# they are still iterated over line-by-line, and the boundaries between keys must be detected by the user.
#
# disable warning so they want
options(warn=-1)
# redirect output
sink("/dev/null")

prev_key <- ''
dates_vector <- vector()
count_vector <- vector()
avg_vector <- vector()

conn <- file("stdin", open="r")
while (length(next.line <- readLines(conn, n=1)) > 0) {
  # split string by Tab and flatten result into vector
  line <- unlist( strsplit(next.line, "\t") )
  cur_key <- line[1]

  # not first processing and key was changed
  if( prev_key != '' && prev_key != cur_key ) {
    # get prediction for tweets count
    mod_count <- lm(count_vector ~ dates_vector)
    rep_date <- dates_vector[length(dates_vector)]
    rep_date <- rep_date+60*60
    prediction_count <- predict(mod_count, data.frame( dates_vector=c( rep_date )))


    # get prediction for average sentiment amount
    mod_avg <- lm(avg_vector ~ dates_vector)
    prediction_avg <- predict(mod_avg, data.frame( dates_vector=c( dates_vector[length(dates_vector)]+1 ) ))

    sink()
    cat(prev_key,  format( rep_date, format="%Y%m%d%H%M" ), floor( prediction_count ),round(prediction_avg, digits=2 ), "\n", sep="\t")
    sink("/dev/null")

    # new key - new vectors
    dates_vector <- vector()
    count_vector <- vector()
    avg_vector <-  vector()
  }

  prev_key <- cur_key

  # setup values from passed line
  dates_vector <- append(dates_vector, as.POSIXct(line[2], tz="0", format="%Y-%m-%d %H:%M:%S"))
  count_vector <- append(count_vector, as.numeric(line[3]))
  avg_vector<- append(avg_vector, as.double(line[4]))
}
close(conn)
