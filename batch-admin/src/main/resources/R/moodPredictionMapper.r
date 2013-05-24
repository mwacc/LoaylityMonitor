#! /usr/bin/env Rscript
# disable warning so they want
options(warn=-1)
# redirect output
sink("/dev/null")

conn <- file("stdin", open="r")
while (length(next.line <- readLines(conn, n=1, warn=FALSE)) > 0) {
  # split string by Tab and flatten result into vector
  line <- unlist( strsplit(next.line, "\t") )

  # key consist category
  output.key <- c(line[2])
  # value consist datetime, count of mentioning and average sentiment score
  output.value <- c(line[1], line[3], line[4])

  # restore output
  sink()
  cat(output.key, output.value, "\n", sep="\t")
  sink("/dev/null")
}
close(conn)