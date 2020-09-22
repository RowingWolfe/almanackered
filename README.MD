## Almanackered
Written because I am broke and needed historical weather data for 
Mantilogs.

Pulls weather data for given zip code between date ranges from alamanac.com and stores 
it in a json file.

Uses threading to make it a little faster, don't try to push the
thread_count too high, might get done for DDoS, I know at 10 threads I got a lot
of failures trying to fetch the data so I settled on a nice, safe 5.

