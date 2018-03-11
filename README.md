# dynamodbtopg
Data migration from dynamodb to pg.
Nested data (dict, list) are in their literal sytax.


<h2>Usage:</h2>

python dypg.py -r ap-southeast-1 -dt Snowplow-s3-loader -hs localhost -u farhah -d naluri


-r = Region<br>
-dt = dynamo db table name<br>
-hs = psql host<br>
-u = psql username<br>
-d = psql database<br>

**These are the compulsory args

---

<h5>Complete args are:</h5>

-r = Region<br>
-dt = dynamo db table name<br>
-hs = psql host<br>
-p = psql port<br>
-u = psql username<br>
-d = psql database<br>
-pass = psql pass<br>
-s = psql schema<br>
