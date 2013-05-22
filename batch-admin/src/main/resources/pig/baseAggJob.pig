-- REGISTER parsing jars
REGISTER '$pathToSimpleJson';
REGISTER '$pathToElephantBird';


SET  default_parallel $parallel
SET  mapred.job.name 'Base Twitter aggregation job'
SET  pig.tmpfilecompression false

-- it works in production, but custom loader doesn't work with PigUnit
--DEFINE TweetsLoader com.twitter.elephantbird.pig.load.JsonLoader();
DEFINE StringToTweet com.twitter.elephantbird.pig.piggybank.JsonStringToMap();

raw_line = LOAD '$input' AS (line:CHARARRAY);
json = FOREACH raw_line GENERATE StringToTweet(line);
tweets = FOREACH json GENERATE $0#'text' AS text, $0#'created_at' AS timestamp;

STORE tweets INTO '$output';