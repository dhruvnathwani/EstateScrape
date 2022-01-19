import stripe
from flask import Flask, request, jsonify
import pymongo
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import requests
import pandas as pd
import io
from uszipcode import SearchEngine 
from mortgage import Loan
import numpy as np 
from fuzzywuzzy import process
import json 
import time

# INPUTS

# Stripe API Key
stripe.api_key = ''

# Apify API Key
api_key = ''

# MongoDB Server
mobgoDBConnection = ''

# Email Parameters
emailPassword = ''
emailAddress = ''

# Validation email to also send to in case you want to track data yourself
validationEmail = ''

def validate_city(city, state):
    citydata = pd.read_csv('uscities.csv')

    checker = dict(zip(citydata['state_id'], citydata['state_name']))

    lookups = list(pd.unique(citydata['lookup']))

    # If the state is less than 2 letters, find it's corresponding long state

    if len(state) <= 2:

        # We need to find the valid lookup for it's abbreviation

        state = checker[state]

    query = str(city) + ',' + str(state)

    output = process.extractOne(query, lookups)

    # If the score is less than 95, it is invalid, otherwise return the correct spelling
    if output[1] < 95:
        return 'invalid'
    else:
        return output[0]

def validate_zip_code(zipCode):
    zips_file = pd.read_csv('zips.csv')

    zips = list(zips_file['zip'].astype(str))

    if zipCode in zips:
        result = zipCode

    else:
        result = 'invalid'

    #search = SearchEngine()
    #zipcode = search.by_zipcode(zipCode)
    #try: 
    #    result = zipcode.post_office_city

        # If there is no corresponding post office city, it is invalid
    #    if result == None:
    #        result = 'invalid'
    #    else:
    #        result = zipCode
    #except:
    #    result = 'invalid'
    return result

def validate_county_update(county, input_state):
    
    # Read in counties
    info = open('counties', 'r')

    out = info.readlines()

    info.close()

    counties = out[0].split(";")

    # Read in state dictionary
    with open('states.json') as f:
        states = json.load(f)
        
    abbrevs = {}

    for state in states:

        abbrevs[state['Code']] = state['State']

    # If the word ends in county, no need to worry, otherwise add county to the end of it
    try:

        last_word = county.split(" ")[-1]

        if last_word.lower() != 'county':

            county = county + str(' County')
    
    except:

        county = county

    # If the state is abbreviated, find the long version of that state

    if len(input_state) <= 2:
    
        search_state = abbrevs[input_state]
    
    else:

        search_state = input_state

    query = str(county) + "," + str(search_state)

    output = process.extractOne(query, counties)

    # If the score is less than 95, it is invalid, otherwise return the correct spelling
    if output[1] < 95:
        return 'invalid'
    else:
        return output[0]

def send_email(send_from, password, send_to, subject, body):
    multipart = MIMEMultipart()
    multipart['From'] = send_from
    multipart['To'] = send_to
    multipart['Subject'] = subject   
    multipart.attach(MIMEText(body, 'html'))
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(send_from, password)
    s.sendmail(send_from, send_to, multipart.as_string())
    s.quit()

def handleSubscription(request):

    # Parse the incoming data from the slack request
    resp = request.form

    resp = request.form.to_dict()

    # Parse response and store it into a dictionary
    resp = resp['text'].replace("There is a new form submission!", "")
    resp = resp.replace("\n", "")

    resp = resp.split(";")

    out = {}

    # Parse message from slackbot into dictionary
    for x in resp:
        x = x.split(":")

        if x[0].strip() == 'Email':
            out[x[0].strip()] = x[2].strip().split("|")[0]
        
        else:
            out[x[0].strip()] = x[1].strip()

    email = out['Email']
    first_name = out['First Name']

    client = pymongo.MongoClient(mobgoDBConnection)

    db = client.orders.subs


    # Get list of stripe subscriptions submitted in the last 20 seconds
    epoch = (time.time()) - 20

    epoch = int(epoch)

    # All subscriptions created in the last 20 seconds, find the first item in that list and get the subscription id
    subs = stripe.Subscription.list(created={'gte': epoch})

    subId = subs['data'][0]['id']

    out['subId'] = subId

    # Split by output type for proper validation, pass validation into result variable
    if out['Type'] == 'City Subscription':

        city = out['City to Scrape']
        state = out['State to Scrape']

        result = validate_city(city, state)

    elif out['Type'] == 'Zip Code Subscription':

        zipCode = out['Zip Code to Scrape']

        result = validate_zip_code(zipCode)
    
    else:

        county = out['County to Scrape']
        state = out['State to Scrape']

        result = validate_county_update(county, state)

    if result == 'invalid':

        # Email me to let me know there was an input issue
        send_email(emailAddress, emailPassword, validationEmail, 'Location input error for ' + str(first_name) + ' at ' + str(email), 'Double check the input for ' + str(email) + ' to make sure it was input correctly. If there is any issue, make sure to reach out to the user')

    out['location'] = result

    out['defaultDatasetId'] = 'None'

    out['Email Sent'] = 'N'

    # Store all of this information into our subscription database

    db.insert_one(out)

    # Send email to the user to confirm their subscription
    send_email(emailAddress, emailPassword, email, 'Welcome to EstateScrape!', 'Hey ' + str(first_name) + ', <br> <br> Thank you for choosing EstateScrape! <br> <br> Your first 7 days are on us, but if you decide you want to cancel at any time, feel free to send us an email at estatescrape@gmail.com and one of our customer service representatives can help you out! <br> <br> Best, <br> <br> The EstateScrape Team')

    # Send email to me to let me know there is a new subscriber
    send_email(emailAddress, emailPassword, validationEmail, 'There is a new subscriber!', 'Hey, <br> <br> You have a new subscriber! <br> <br> ' + str(first_name) + ' ( ' + str(email) + ') has subscribed to ' + str(result) + '.')

def dailyScrapeRun(request):

    # Every day run this code

    client = pymongo.MongoClient(mobgoDBConnection)

    db = client.orders.subs

    # Find all of the entries in the database
    docs = list(db.find({}))

    # Consolidator dictionary to map subId's to their location to take advantage of overlaps
    # Ex, if two people are scraping LA, we want to only send one request for LA

    consolidator = {}

    for x in docs:

        # Check if the subscription is active, otherwise go to the next item in the database

        valid = ['active', 'trialing']

        subId = x['subId']

        info = stripe.Subscription.retrieve(subId)

        status = info['status']

        if status not in valid:
            next

        else:

            # First, update the email sent parameter to show that it is not yet sent
            db.find_one_and_update({'subId': subId}, {"$set": {"Email Sent": 'N'}}, upsert=False)

            # Extract the location and consolidate it
            location = x['location']

            
            keys = list(consolidator.keys())

            # If the location does not exist in the dictionary, make it an empty list
            if location not in keys:

                consolidator[location] = []
            
                
            # Add to the list
            consolidator[location].append(subId)

    # Now that we have a consolidated list of what we need to search, we can pass these to the scraper

    queries = list(consolidator.keys())

    # For each location, pass it to the scraper
    for search_location in queries:

        # Send a request to the scraper
                
        payload = {
            
            "search": search_location,
            "type": "sale",
            "maxLevel": 20,
            "maxItems": 50000,
            "simple": False,
            "extendOutputFunction": "(data) => {\n    return {};\n}",
            "proxyConfiguration": {
            "useApifyProxy": True
            }
        }

        headers= {'content-type': 'application/json'}

        req = requests.post('https://api.apify.com/v2/acts/petr_cermak~zillow-api-scraper/runs?token=' + api_key, headers=headers, json=payload, timeout=None)

        output = req.json()

        defaultDatasetId = output['data']['defaultDatasetId']

        # For each unique subscription mapped to the location, update the defaultDatasetId
        for subId in consolidator[search_location]:

            db.find_one_and_update({'subId': subId}, {"$set": {"defaultDatasetId": defaultDatasetId}}, upsert=False)


        








        




