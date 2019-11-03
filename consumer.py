#!/usr/bin/env python
import pika
import sqlite3
import csv
import json
from xml.etree import ElementTree

# all the required queries
INVOICE_BY_COUNTRY = "SELECT BillingCountry as Country , count(BillingCountry) as NumOfPurchases \
    from invoices group by BillingCountry"
CREATE_TABLE_INVOICE_BY_COUNTRY = "CREATE TABLE IF NOT EXISTS ans1 as " + INVOICE_BY_COUNTRY

QUANTITY_BY_COUNTRY = "SELECT BillingCountry as Country ,sum(quantity) as QuantityOfPurchases \
    from invoice_items,invoices where invoice_items.invoiceid=invoices.invoiceid group by BillingCountry"
CREATE_TABLE_QUANTITY_BY_COUNTRY= "CREATE TABLE IF NOT EXISTS ans2 as " + QUANTITY_BY_COUNTRY

ALBUMS_BY_COUNTRY ="SELECT BillingCountry as Country, group_concat(distinct albums.title) as AlbumsList \
    from albums inner join tracks on albums.albumid = tracks.albumid inner join invoice_items \
        on tracks.trackid = invoice_items.trackid inner join invoices \
            on invoice_items.invoiceid = invoices.invoiceid group by BillingCountry"

MOST_POPULAR_ROCK_ALBUM_BY_YEAR_AND_COUNTRY ="select title,country,MAX(quantity) as numOfCopies ,{0} \
    as Year from (select albums.title as title,invoices.BillingCountry \
        as country ,SUM(Quantity) as quantity ,{0} as years from albums inner join tracks \
            on albums.albumid=tracks.albumid inner join genres \
                on tracks.genreid = genres.genreid inner join invoice_items \
                    on tracks.trackid = invoice_items.trackid inner join invoices \
                        on invoice_items.invoiceid=invoices.invoiceid where {0} <= strftime('%Y',invoicedate) \
                            and invoices.BillingCountry ='{1}' and genres.name ='Rock' \
                                group by title,country,quantity,years)"
CREATE_TABLE_MOST_POPULAR_ROCK_ALBUM_BY_YEAR_AND_COUNTRY = "CREATE TABLE IF NOT EXISTS ans4_{2} as " + MOST_POPULAR_ROCK_ALBUM_BY_YEAR_AND_COUNTRY

#csv format rules
csv.register_dialect('myDialect',
    delimiter = ',',
    quoting = csv.QUOTE_NONE,
    skipinitialspace = True)

#create SQLite connection
connectionSQL = sqlite3.connect('chinook.db')
cursor = connectionSQL.cursor()

#RabbitMQ connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host = 'localhost')
)

channel = connection.channel()

if (not channel):
    print("Connection error!\n")

#create csv for question 2.1
def csv1(rows):
    with open('ans1.csv', 'w') as f:
        writer =csv.writer(f, dialect='myDialect')
        writer.writerow(['Country','num of invoices'])
        for row in rows:
            writer.writerow(row)
            

#create csv for question 2.2
def csv2(rows):
    with open('ans2.csv', 'w') as f:
        writer =csv.writer(f, dialect='myDialect')
        writer.writerow(['Country','Quantity'])
        for row in rows:
            writer.writerow(row)
            

#create json for question 2.3
def json3(rows):
    with open('./'+'ans3.json', 'w') as f:
        full_data = []
        for row in rows:
            data = {}
            data[row[0]]=[]
            data[row[0]].append({
               'Albums': row[1] 
            })
            full_data.append(data)
        json.dump(full_data, f)

#create xml for question 2.4
def xml4(rows):
    for row in rows:
        tree = ElementTree.ElementTree()
        root = ElementTree.Element("root")
        album_name = ElementTree.Element("Album")
        album_name.text = row[0]
        root.append(album_name)
        country_name = ElementTree.Element("Country")
        country_name.text = row[1]
        root.append(country_name)
        quantity = ElementTree.Element("Quantity")
        quantity.text = str(row[2])
        root.append(quantity)
        from_year = ElementTree.Element("FromYear")
        from_year.text = str(row[3])
        root.append(from_year)
        tree._setroot(root)
        new_row1=row[1]
        if ' ' in row[1]:
            new_row1=row[1].replace(' ', '_')
        tree.write("ans4"+new_row1+str(row[3])+".xml")
            
#Parse and process the message recieved
def callback(ch, method, properties, body):
    msg_data_str = body.decode('utf-8')
    msg_data = msg_data_str.split(',')
    year = msg_data[0]
    country = msg_data[1]
    db_path = msg_data[2]
    print("[x] Received %r" %body)

    #csv1
    cursor.execute(INVOICE_BY_COUNTRY)
    rows = cursor.fetchall()
    csv1(rows)
    cursor.execute(CREATE_TABLE_INVOICE_BY_COUNTRY)
    
    #csv2
    cursor.execute(QUANTITY_BY_COUNTRY)
    rows = cursor.fetchall()
    csv2(rows)
    cursor.execute(CREATE_TABLE_QUANTITY_BY_COUNTRY)

    #json3
    cursor.execute(ALBUMS_BY_COUNTRY)
    rows = cursor.fetchall()
    json3(rows)

    #xml4
    ans4 = MOST_POPULAR_ROCK_ALBUM_BY_YEAR_AND_COUNTRY.format(year,country)
    cursor.execute(ans4)
    rows = cursor.fetchall()
    xml4(rows)
    no_space_country = country
    # if country contains more than one word
    if ' ' in country:
            no_space_country = country.replace(' ', '_')
    create_table_ans4 = CREATE_TABLE_MOST_POPULAR_ROCK_ALBUM_BY_YEAR_AND_COUNTRY.format(year,country,no_space_country)
    cursor.execute(create_table_ans4)
    connectionSQL.commit()

channel.basic_consume(
    queue='hello', on_message_callback=callback, auto_ack=True
)

print('[*] Waiting for messages to exit press ctrl + c')
channel.start_consuming()

