# dynamodbtopg
Data migration from dynamodb to pg.
Nested data (dict, list) are in their literal sytax.


Usage:

python dypg.py -r ap-southeast-1 -dt Snowplow-s3-loader -hs localhost -u farhah -d naluri


-r = Region
-dt = dynamo db table name
-hs = psql host
-u = psql username
-d = psql database
