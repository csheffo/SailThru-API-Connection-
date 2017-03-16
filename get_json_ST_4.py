import os
import sys
import json

from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_response import SailthruResponseError
from sailthru.sailthru_error import SailthruClientError

import boto
import boto.s3

from secrets import *
from boto.s3.connection import OrdinaryCallingFormat, S3Connection

api_key = ST_API_KEY
api_secret = ST_API_SECRET
sailthru_client = SailthruClient(api_key, api_secret)

def get_newsletter_count_sailthru(newsletter):
    try:
            response = sailthru_client.api_get("stats", {"stat": "list" , "list": newsletter, "date": "2017-03-15"})

            if response.is_ok():
                try:
                     body = response.get_body()
                     list_data = body
                     return list_data

                except:
                    print "API call was good but there was a problem cleaning the data."
            else:
                print "There was a problem with the API call for %s at index %i", newsletter, i
                error = response.get_error()
                print ("Error: " + error.get_message())
                print ("Status Code: " + str(response.get_status_code()))
                print ("Error Code: " + str(error.get_error_code()))


    except SailthruClientError as e:
        # Handle exceptions
        print ("Exception")
        print (e)

def get_blast_open_rate_sailthru(newsletter):
    try:
            response = sailthru_client.api_get("stats", {"stat": "blast" , "list": newsletter, "start_date": '2017-03-15' , "end_date": '2017-03-15'})

            if response.is_ok():
                try:
                     body = response.get_body()
                     blast_data = body
                     #print blast_data
                     return blast_data

                except:
                    print "API call was good but there was a problem cleaning the data."
            else:
                print "There was a problem with the API call for " + newsletter
                error = response.get_error()
                print ("Error: " + error.get_message())
                print ("Status Code: " + str(response.get_status_code()))
                print ("Error Code: " + str(error.get_error_code()))


    except SailthruClientError as e:
        # Handle exceptions
        print ("Exception")
        print (e)


def clean_up_json(newsletter, list_data, blast_data):
    try:
        print "Attempting to clean up JSON data..."
        opens_numberater =  float(blast_data['estopens'])
        opens_denominator =  float(blast_data['count'])
        open_rate = opens_numberater / opens_denominator

        cleaned_json = {
            newsletter + '-email_count': list_data['email_count'],
            newsletter + '-engaged_count': list_data['engaged_count'],
            newsletter + '-new_count': list_data['new_count'],
            newsletter + '-open_rate': open_rate,
            newsletter + '-clicks': blast_data['click_total']
            #based on Nick response may also need to pull based on urls with urls = 1
        }

        print "Success!"

        return cleaned_json
    except Exception, e:
        print "Error cleaning up JSON"
        print e

#strips out blast data from cleaning function and reports NA for days alerts didn't send
def clean_up_json_no_blast(newsletter, list_data):
    try:
        print "Attempting to clean up JSON data..."
        #figure out why this is returning none and then you're done
        cleaned_json = {
            newsletter + '-email_count': list_data['email_count'],
            newsletter + '-engaged_count': list_data['engaged_count'],
            newsletter + '-new_count': list_data['new_count'],
            newsletter + '-open_rate': 'NA',
            newsletter + '-clicks': 'NA'
        }
        print "Success!"
        return cleaned_json
    except Exception, e:
        print "Error cleaning up JSON"
        print e

def create_json_file(all_data):

    flattened_data = {}
    for newsletter in all_data:
        for key, value in newsletter.iteritems():
            flattened_data[key] = value
    print flattened_data

    try:
        print "Attempting to create JSON file..."

        with open('sailthru-cache.json', 'w') as outfile:
            json.dump(flattened_data, outfile)
        #print outfile
        print "Success!"
    except Exception, e:
        print "Error creating JSON file"
        print e


def upload_file_to_s3():
    try:
        conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, calling_format=OrdinaryCallingFormat())
        bucket = conn.get_bucket('axios-analytics-data')
        filename = 'sailthru-cache.json'
        k = boto.s3.key.Key(bucket)
        k.key = 'sailthru-cache.json'
        k.set_contents_from_filename(filename)
        # check that file exists
        # then do the push to S3 stuff
        # huzzah, everything went perfectly?
    except Exception, e:
        print "Error uploading file to S3"
        print e

def run_all_the_things():
    newsletters = ['newsletter_axiosam', 'newsletter_axiosprorata' , 'newsletter_axiosvitals' , 'newsletter_axiosgenerate' , 'newsletter_axioslogin', 'alerts_healthcare' , 'alerts_politics' ]

    newsletters_no_blast = ['alerts_business' , 'alerts_technology' , 'newsletter_axiossneakpeek']

    all_data = []


    for i in range(0, len(newsletters)):
        try:
            newsletter = newsletters[i]
            list_data = get_newsletter_count_sailthru(newsletter)
            blast_data = get_blast_open_rate_sailthru(newsletter)
            cleaned_json = clean_up_json(newsletter, list_data, blast_data)
            all_data.append(cleaned_json)
        except:
            print 'something went wrong'

    for i in range(0,len(newsletters_no_blast)):
        try:
            newsletter = newsletters_no_blast[i]
            list_data = get_newsletter_count_sailthru(newsletter)
            cleaned_json = clean_up_json_no_blast(newsletter, list_data)
            all_data.append(cleaned_json)
        except:
            print 'something went wrong'


    create_json_file(all_data)
    upload_file_to_s3()
    print "Everything went well and the data is updated!"




run_all_the_things()
