## Almanackered
Written with python 3.8 because I am broke and needed historical weather data for 
Mantilogs (Weather APIs be expensive, yo.).

I'll be updating it and adding more features when I migrate Mantilogs to Postgres.


Pulls weather data for given zip code between date ranges from alamanac.com and stores 
it in a json file.

Uses threading to make it a little faster, don't try to push the
thread_count too high, might get done for DDoS, I know at 10 threads I got a lot
of failures trying to fetch the data so I settled on a nice, safe 5.

Required libs in requirements.txt

Now with 100% more SQLite export! It's basic, but it does the job for what I need.

Just run sqlite-export.py