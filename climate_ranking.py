"""A quickly written scraper to extract data from temp-and-precip/climatological-rankings

requires: requests, lxml, and grequests to run
Look to pip or your package panager.

it's likely to take a while to run as it looks like it will require
about 200 million http requests.

It'd be really nice if sites would let you request more than one data point at a time.

Output is a set of of csv files by year.

The title column is a human readable version of the request url form parameters
"""

from __future__ import print_function

import requests
from lxml.html import fromstring
import itertools
import grequests
import time

def get_parameters_from_landing():
    landing_url = 'https://www.ncdc.noaa.gov/temp-and-precip/climatological-rankings/'

    page = requests.get(landing_url)

    tree = fromstring(page.content)

    selects = tree.xpath('//select')

    parameters = {}

    for select in selects:
        name = select.attrib['name']
        values = {}
        for child in select.getchildren():
            value = child.attrib['value']
            values[value] = child.text
        parameters[name] = values    
    return parameters

def forgiving_handler(request, exception):
    print('Error {} on {}'.format(str(exception), request.url))
    
def get_form_parameters(parameters):
    """Return iterators over all the parameters except for years
    """
    #parameter=tavg&state=110&div=0&month=11&periods[]=1&year=2013
    yield from itertools.product(
        sorted(parameters['parameter'].keys()),
        sorted(parameters['state'].keys()),
        sorted(parameters['div'].keys()),
        sorted(parameters['month'].keys()),
        sorted(parameters['periods[]'].keys()),
    )

def get_data(parameters):
    baseurl = 'https://www.ncdc.noaa.gov/temp-and-precip/climatological-rankings/download.csv'
    parameter_columns = ['parameter', 'state', 'div', 'month', 'periods[]', 'year']
    csvheader = "Title,Period,Value,Twentieth Century Mean, Departure, Low Rank, High Rank, Record Low, Record High, Lowest Since, Highest Since, Percentile, Ties"

    # grab more interesting years first
    for year in parameters['year']:
        print('starting {}'.format(year)) 
        tzero = time.monotonic()

        with open('climatological_rankings_{}.csv'.format(year), 'wt') as outstream:
            outstream.write(csvheader)
            
            reqs = []
            for val in get_form_parameters(parameters):
                args = zip(parameter_columns, itertools.chain(val, [year]))
                reqs.append(grequests.get(baseurl, params=args))

                if len(reqs) > 20000:

                    results = grequests.imap(reqs, size=20, exception_handler=forgiving_handler)
                    for result in results:
                        if result is not None:
                            line = result.content.decode('utf-8').split('\n')
                            outstream.write(line[0])
                            outstream.write('",')
                            outstream.write(line[2])
                            outstream.write('\n')

                    reqs = []
                    print('.', sep='')

        tnext = time.monotonic()
        print('Processed year {} in {:,} seconds'.format(year, tnext-tzero))
        tzero = tnext

def main():
    parameters = get_parameters_from_landing()
    get_data(parameters)

if __name__ == '__main__':
    main()
