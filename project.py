import pandas as pd
import psycopg2
import csv
import sys

'''
#Convert Food price excel file to csv
data_xls = pd.read_excel('historicalcpi.xls')
data_xls.to_csv('historicalcpi.csv', encoding='utf-8')
'''
# CSV TO dictionary for insertion to database
food_dict = dict()
with open('historicalcpi.csv') as data_csv:
    reader = csv.reader(data_csv)
    row = reader.__next__()
    years = []
    for year in row[1::]:
        years.append(int(year))
    for row in reader:
        food_dict[row[0]] = dict()
        for i in range(len(years)):
            food_dict[row[0]][years[i]] = row[i + 1]

try:
    conn = psycopg2.connect("dbname='ds_project_f17' user='postgres' host='localhost' password=''")
except:
    print(sys.exc_info()[0])


cur = conn.cursor()
query = ""
j = 1
for food in food_dict:
    f = food_dict[food]
    '''
    # Insert food items
    cur.execute("INSERT INTO food_item(name) VALUES ('{}');".format(food))
    
    # Insert price change per year
    for i in range(len(years)):
        query = "INSERT INTO food_price_change_per_year(food_id, percent_change, year) VALUES({}, {}, {});"\
            .format(j, f[years[i]], years[i])
        cur.execute(query)
    j+=1
    '''
conn.commit()
cur.close()
conn.close()