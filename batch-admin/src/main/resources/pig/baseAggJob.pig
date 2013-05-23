-- REGISTER parsing jars
REGISTER '$pathToSimpleJson';
REGISTER '$pathToElephantBird';
REGISTER '$pathToCustomLib';


SET  default_parallel $parallel
SET  mapred.job.name 'Base Twitter aggregation job'
SET  pig.tmpfilecompression false

set mapred.cache.files '/loyalitymonitor/dimcache/categories.txt#categories,/loyalitymonitor/dimcache/sentiments.txt#sentiments';
set mapred.create.symlink 'yes';

-- it works in production, but custom loader doesn't work with PigUnit
--DEFINE TweetsLoader com.twitter.elephantbird.pig.load.JsonLoader();
DEFINE StringToTweet com.twitter.elephantbird.pig.piggybank.JsonStringToMap();
DEFINE ExtractCategory loyalitymonitor.CategoryNumberEvaluator();
DEFINE GetSentiment loyalitymonitor.SentimentsEvaluator();

-- raw_line = LOAD '/loyalitymonitor/data/test/tweet.json' AS (line:CHARARRAY);
raw_line = LOAD '$input' AS (line:CHARARRAY);
json = FOREACH raw_line GENERATE StringToTweet(line);
tweets = FOREACH json GENERATE $0#'text' AS text, $0#'created_at' AS timestamp;
categorized_tweets = FOREACH tweets GENERATE FLATTEN(ExtractCategory(text)) as (category, text), FLATTEN(GetSentiment(text)) as sentiment, timestamp as timestamp;

STORE tweets INTO '$output';