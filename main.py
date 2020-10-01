'''
Almanackered, because fuck the price of weather apis.
All I wanted was a year's data for my gecko project but nope, $10.
I don't have no $10 for 60kb of data. I could eat for a week on that.

I will scrape from here: https://www.almanac.com/weather/history/zipcode/14813/2020-09-16 Behold, the structure.

Note: Do I now use this data for a microservice endpoint or just commit it to a sql db?
While I could just as easily slap it into a NoSQL db and call it a day, perhaps it would be wiser to have it entered
into a weather_history db in mantilogs?
'''

#Todo: Maybe add retry for failed or increase wait time for timeouts.

from datetime import date, timedelta
from bs4 import BeautifulSoup
from queue import Queue
import requests
import json
import threading

# debug imports
import pprint

zip_code = 14813
out_file = "weather_data.json"
# time_delta is how much time to pass between entries.
# Can be set to any delta you want, weeks, days, years.
# Currently it will scan every day.
time_delta = timedelta(days=1)

# Date to start collecting data from
start_day = date(2020, 4, 1)
# Last day to collect data from.
# NOTE: Can only collect up to 1 week before current day. Not my fault, blame the almanac.
# Can get that information other ways though, if needed.
end_day = date(2020, 9, 23)

# A pool of dates in a Queue, for threading.
url_pool = Queue()
thread_list = []
# Threads to run. If set too high you might get done for DDoS. Be careful.
thread_count = 3

# Store the data as a dict. (Dicts act like json for our purposes here and are easy to convert.)
weather_data = {}

print("Almanackered 0.2")
print("If data starts failing to download, re-run a few times. "
      "I will fix this later but it does cache what it already has."
      "So the next run will get more and so on. Sorry about that.")

def generate_url_pool(start_date, end_date):
    ''' Generate a pool of dates from start_date to end_date incremented by time_delta '''
    while start_date <= end_date:
        url_pool.put(url_formatter(start_date, zip_code))
        start_date += time_delta


# Need a function to construct the URLs and make the calls.
def url_formatter(date, zipcode):
    '''Takes a date eg; 2020-09-15 and a zipcode eg; 14813 and returns a url for the almanac for that date and place.'''
    return f"https://www.almanac.com/weather/history/zipcode/{zipcode}/{date}"


def data_fetcher(url):
    '''Takes a URL from the url_formatter() and makes a GET request to the almanac, returns the page.'''
    print("Fetching data for ", url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    content = soup.find('table', {
        "class": "weatherhistory_results"
    })

    # Construct a dict for the day's entry.
    day_weather_dict = {}

    # Get the header for the row. store as desc
    try:
        for tr in content:
            desc = tr.find('th').find('h3', text=True)
            try:
                desc = desc.getText()
            except:
                pass

            # Get the value from the row. store as val
            val = tr.find('span', {
                "class": "value"
            })
            try:
                # Remove the tags from the data.
                val = val.getText()
            except:
                pass

            # print("decc", desc, "val", val)

            # There are no switches in python so we make an ugly bunch of if statements.
            if desc != None and val != None:
                # print("desc", desc, "val", val)
                day_weather_dict[desc] = val
    except:
        print("No weather data here... for some reason", url)
        with open("err.log", 'a') as f:
            f.write("Could not fetch data for: {0} \n".format(url))

    return day_weather_dict


# Thread worker
def extract_data():
    '''Until the url_pool is emptied, the threads will grab urls to work
    with and add an entry to the dict for each date.'''
    while not url_pool.empty():
        url = url_pool.get()
        weather_data[url.split('/')[-1]] = data_fetcher(url)


if __name__ == '__main__':
    # Generate a url pool for extract_data() to pull URLs from.
    generate_url_pool(start_day, end_day)

    # Generate thread list with the worker being extract_data()
    for t in range(thread_count):
        thread = threading.Thread(target=extract_data)
        thread_list.append(thread)

    # Start threads.
    for thread in thread_list:
        thread.start()

    # Do not open file and write to it until all threads finish.
    for thread in thread_list:
        thread.join()

    # Order the data by date lexicographically, convert to json and put it in the out_file.
    weather_json = json.dumps(sorted(weather_data.items()), indent=4)
    with open(file=out_file, mode='w') as f:
        f.write(weather_json)

