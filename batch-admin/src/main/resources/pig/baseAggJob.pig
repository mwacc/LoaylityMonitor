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
DEFINE RoundUpDate loyalitymonitor.TimestampRoundUp('2');

-- raw_line = LOAD '/loyalitymonitor/data/test/tweet.json' AS (line:CHARARRAY);
raw_line = LOAD '$input' AS (line:CHARARRAY);
json = FOREACH raw_line GENERATE StringToTweet(line);
tweets = FOREACH json GENERATE $0#'text' AS text, $0#'created_at' AS timestamp;
categorized_tweets = FOREACH tweets GENERATE
    FLATTEN(ExtractCategory(text)) as (category, text),
    GetSentiment(text) as sentiment,
    RoundUpDate(timestamp) as timestamp;

filtered_tweets = FILTER categorized_tweets BY sentiment != 0;

grouped_tweets = GROUP filtered_tweets BY (timestamp, category);
agg_tweets = FOREACH grouped_tweets GENERATE
    FLATTEN(group) AS (category, timestamp),
    COUNT(filtered_tweets) as count,
    ROUND( AVG(filtered_tweets.sentiment)*100 )/100.0 as average;

STORE agg_tweets INTO '$output';