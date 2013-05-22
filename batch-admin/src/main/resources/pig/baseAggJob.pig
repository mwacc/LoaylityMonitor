-- REGISTER parsing jars
REGISTER '$pathToSimpleJson';
REGISTER '$pathToElephantBird';


SET  default_parallel $parallel
SET  mapred.job.name 'Base Twitter aggregation job'
SET  pig.tmpfilecompression false

DEFINE TweetsLoader com.twitter.elephantbird.pig.load.JsonLoader();

raw = LOAD '$input' USING TweetsLoader AS (json: map[]);
--tweets = FOREACH raw GENERATE json#'text' AS text, json#'created_at' AS timestamp;

STORE raw INTO '$output';