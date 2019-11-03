#!/usr/bin/env python
import pika
import re

DB_PATH =  'chinook.db'
BODY = "{0},{1},{2}"

#A function that ensures that the user enters proper country input
def country_from_user():
    countries = "Germany,Norway,Belgium,Canada,USA,Ireland,United Kingdom,Australia,Chile,India,Brazil,Portugal,Netherlands,Spain,Sweden,Czech Republic,Finland,Denmark,Italy,Poland,Austria,Hungary,Argentina,France"
    each_country = countries.split(',')
    country = input("Please choose country from this list:\n" + countries + "\n")
    while country not in each_country:
        print("Please make sure you enter a valid country\n")
        country = input("Please choose country from this list:\n" + countries + "\n")
    return country

#A function that ensures that the user enters proper year input
def year_from_user():    
    year = input("Enter year: please make sure you enter 4 digits. for example - 2005\n")
    while not (re.match(r'^-?[0-9]+$', year) and len(year) == 4 and year[0] in('0','1','2') and int(year) < 2020):
        print ("Please make sure you enter a valid year\n")
        year = input("Enter year: please make sure you enter 4 digits. for example - 2005\n")
    return year

#RabbitMQ connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)

channel = connection.channel()
if (not channel):
    print("Connection error!\n")

channel.queue_declare(queue = 'hello')
channel.basic_publish(exchange='', routing_key='hello', body = BODY.format(year_from_user(), country_from_user(), DB_PATH))

channel.close()

