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
            response = sailthru_client.api_get("stats", {"stat": "list" , "list": newsletter, "date": "2017-03-13"})

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
            response = sailthru_client.api_get("stats", {"stat": "blast" , "list": newsletter, "start_date": "2017-03-13" , "end_date": "2017-03-13"})

            if response.is_ok():
                try:
                     body = response.get_body()
                     blast_data = body
                     #print blast_data
                     return blast_data

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

def clean_up_json(newsletter, list_data, blast_data):
    try:
        print "Attempting to clean up JSON data..."

        opens_numberater =  float(blast_data['estopens'])
        opens_denominator =  float(blast_data['count'])
        open_rate = opens_numberater / opens_denominator

        cleaned_json = {newsletter : { 'email_count' : blast_data['count'] , 'engaged_count' :        list_data['engaged_count'] , 'new_count' : list_data['new_count'] , 'open_rate': open_rate}}

        print "Success!"
        return cleaned_json
    except Exception, e:
        print "Error cleaning up JSON"
        print e

def aggregate_cleaned_json(newsletter, cleaned_json):
    #this function should take the cleaned json for all the rounds of the for loop as inputs and make it one level of cleaned up json
    #count and open rate both coming from the cleaned json in clean_up_json
    #only working with these two now for simplicity because these are the essential ones
    count = cleaned_json[newsletter]['email_count']
    open_rate = cleaned_json[newsletter]['open_rate']
    #this block is to try to correctly label the metrics going in to bime
    if newsletter == 'newsletter_axiosam':
        #depending in specific behavior this may need to be .extend instead of .append
        aggregate_cleaned_json.append('Axios AM subscriber count': count , 'Axios AM open rate': open_rate)
    else if newsletter == 'newsletter_axiosprorata':
        aggregate_cleaned_json.append('Axios Pro Rata subscriber count': count , 'Axios Pro Rata open rate': open_rate)
    else if newsletter == 'newsletter_axiosvitals':
        aggregate_cleaned_json.append('Axios Vitals subscriber count': count , 'Axios Vitals open rate': open_rate)
    else if newsletter == 'newsletter_axiossneakpeek':
        aggregate_cleaned_json.append('Axios Sneak Peek subscriber count': count , 'Axios Sneak Peek open rate': open_rate)
    else if newsletter == 'newsletter_axiosgenerate':
        aggregate_cleaned_json.append('Axios Generate subscriber count': count , 'Axios Generate open rate': open_rate)
    else if newsletter == 'newsletter_axioslogin':
        aggregate_cleaned_json.append('Axios Login subscriber count': count , 'Axios Login open rate': open_rate)
    else if newsletter == 'alerts_business':
        aggregate_cleaned_json.append('Business Alerts subscriber count': count , 'Business Alerts open rate': open_rate)
    else if newsletter == 'alerts_healthcare':
        aggregate_cleaned_json.append('Healthcare Alerts subscriber count': count , 'Healthcare Alerts open rate': open_rate)
    else if newsletter == 'alerts_politics':
        aggregate_cleaned_json.append('Politics Alerts subscriber count': count , 'Politics Alerts open rate': open_rate)
    else:
        aggregate_cleaned_json.append('Technology Alerts subscriber count': count , 'Technology Alerts open rate': open_rate)

    print aggregate_cleaned_json
    #this is what I want to be the single level json object to pass to Bime 
    return aggregate_cleaned_json


def create_json_file(cleaned_json):
    try:
        print "Attempting to create JSON file..."
        with open('sailthru-cache.json', 'a') as outfile:
            json.dump(cleaned_json, outfile)
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
    newsletters = ['newsletter_axiosam', 'newsletter_axiosprorata', 'newsletter_axiosvitals' , 'newsletter_axiossneakpeek' , 'newsletter_axiosgenerate' , 'newsletter_axioslogin', 'alerts_business' , 'alerts_healthcare' , 'alerts_politics' , 'alerts_technology']

    aggregate_cleaned_json = []

    for i in range(0, len(newsletters)):
        newsletter = newsletters[i]
        try:
            list_data = get_newsletter_count_sailthru(newsletter)
            blast_data = get_blast_open_rate_sailthru(newsletter)
            cleaned_json = clean_up_json(newsletter, list_data, blast_data)
            aggregate_cleaned_json(newsletter, cleaned_json)
            print "Everything went smoothly and the data was updated."
        except:
            print "Something went wrong"

    #create_json_file(aggregate_cleaned_json)
    #upload_file_to_s3()


run_all_the_things()
