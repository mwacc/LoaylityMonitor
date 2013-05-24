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

  cat(prev_key, cur_key, '\n', sep='\t')
  # same key or first processing
  if( prev_key == '' | prev_key == cur_key ) {
    prev_key <- cur_key
    dates_vector <- append(dates_vector, as.numeric(line[2]))
    count_vector <- append(count_vector, as.numeric(line[3]))
    avg_vector<- append(avg_vector, as.double(line[4]))
  # found a boundary; emit prediction
  } else {
    # get prediction for tweets count
    mod_count <- lm(count_vector ~ dates_vector)
    prediction_count <- predict(mod_count, data.frame( dates_vector=c( dates_vector[length(dates_vector)]+10 ) ))

    # get prediction for average sentiment amount
    mod_avg <- lm(avg_vector ~ dates_vector)
    prediction_avg <- predict(mod_avg, data.frame( dates_vector=c( dates_vector[length(dates_vector)]+10 ) ))

    sink()
    cat(prev_key, dates_vector[length(dates_vector)]+2, prediction_count, "\n", sep="\t")
    cat(prev_key, dates_vector[length(dates_vector)]+2, prediction_avg, "\n", sep="\t")
    sink("/dev/null")
  }

}
close(conn)