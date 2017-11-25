import pandas as pd
import psycopg2
import csv
import sys
import googlemaps
import ds_proj_app_settings as ds


def convert_cpi():
    """
    Converts the consumer price index dataset or the producer price index dataset into a csv then to a dictionary.
    :return: Returns a dictionary of food items and price changes by year
    """

    # Convert Food price excel file to csv
    data_xls = pd.read_excel('../data_sets/cpi_1974-2016.xls')  # historicalcpi.xls')
    data_xls.to_csv('../data_sets/cpi_1974-2016.csv', encoding='utf-8')

    # CSV TO dictionary for insertion to database
    food_dict = dict()
    with open('../data_sets/cpi_1974-2016.csv') as data_csv:
        reader = csv.reader(data_csv)
        row = reader.__next__()
        years = []
        for year in row[1::]:
            years.append(int(year))
        for row in reader:
            food_dict[row[0]] = dict()
            for i in range(len(years)):
                food_dict[row[0]][years[i]] = row[i + 1]
    return food_dict


def create_cpi_query(food_dict, con):
    """
    Produces the concatenated query string for all items in food_dict. Attempts to insert food_item first and returns
    id on success and failure which is used to insert into ppi or cpi table.
    :param food_dict: The dictionary of food price indices
    :param con: The database connection string
    :return: A string containing queries to insert each entry
    """
    query = ''
    table_name = 'pricing_indexes."cpi_1974-2016"'  # 'cpi_1974_2016'
    for food in food_dict:
        qry = "INSERT INTO look_up_tables.food_item_descriptions (food_item_description) VALUES ('{}') ON CONFLICT (food_item_description) DO UPDATE SET food_item_description=EXCLUDED.food_item_description RETURNING food_item_code;".format(food)
        try:
            food_id = run_query(qry, con)
        except:
            pass

        for year in food_dict[food]:
            if food_dict[food][year] == '':
                food_dict[food][year] = 'NULL'

            query += 'INSERT INTO {}(food_item_code, percent_change, year) VALUES ({}, {}, {});'\
                .format(table_name, food_id[0][0], food_dict[food][year], year)
    return query+ "select 1;"


def run_query(query, con, num_records=False):
    """
    Executes a query on a Postgres database
    :param query: The query to be run. Should already be sanitized.
    :param con: The connection to the database.
    :param num_records: Optional. The number of records to return. If not
        specified the entire results set will be returned.
    :return: Returns the results of the query as a list of tuples. If no
        results returns an empty list.
    """

    ''' 
        Create a cursor for executing the query, execute the query, commit or
        rollback the query. If a number of records to return was provided use it, else
        use nothing.
    '''
    cur = con.cursor()
    cur.execute(query)
    con.commit()  # rollback  # Use commit to write to the database, rollback to test.
    if not num_records:
        ret = cur.fetchall()
    else:
        ret = cur.fetchmany(num_records)
    # Always close your connections when you are finished.
    cur.close()

    return ret


def convert_NAICS(con):
    # Convert NAICS codes excel file to csv
    data_xls = pd.read_excel('../data_sets/6-digit_NAICS_2017_Codes.xlsx')  # historicalcpi.xls')
    data_xls.to_csv('../data_sets/6-digit_NAICS_2017_Codes.csv', encoding='utf-8')

    # CSV TO dictionary for insertion to database
    query = b''
    with open('../data_sets/6-digit_NAICS_2017_Codes.csv') as data_csv:
        reader = csv.reader(data_csv)
        cur = con.cursor()
        for line in reader:
            query += cur.mogrify("INSERT INTO look_up_tables.naics_descriptions(naics_code, naics_description) VALUES(%s, %s);", (int(line[1]), line[2].strip()))
    cur.close()
    print(query)
    return run_query(query + "select 1;", con)


def get_lat_long(locations):
    k = ds.google_maps_api_key
    gmaps = googlemaps.Client(key=k)
    locls = {}
    for i in range(len(locations)):
        location = locations[i]
        req = "{}, {}".format(location[0], location[1])
        
        res = gmaps.geocode(req)
        locls[req] = {'state': location[0], 'state_fips_code': location[3], 'county': location[1], 'county_code': location[2], 'lat': res[0]['geometry']['location']['lat'], 'long': res[0]['geometry']['location']['long']}
    return locls


def get_locations_query(con):
    query = "SELECT county_description, state_alpha, county_code, look_up_tables.state_description.state_fips_code FROM look_up_tables.county_descriptions, look_up_tables.state_description WHERE county_descriptions.state_fips_code = state_description.state_fips_code;"
    return(run_query(query, con))


def write_lat_longs(locations, con):
    query = ''
    for location in locations:
        l = locations[location]
        query += "UPDATE look_up_tables.county_descriptions SET latitude = {}, longitude = {} WHERE state_fips_code = {} AND county_code = {}".format(l['lat'], l['long'], l['state_fips_code'], l['county_code'])
    return(run_query(query + 'SELECT 1=1;', con))


def convert_oil():
    """
    Converts the consumer price index dataset or the producer price index dataset into a csv then to a dictionary.
    :return: Returns a dictionary of food items and price changes by year
    """

    # Convert Food price excel file to csv
    data_xls = pd.read_excel('../data_sets/refined_ny_harbor_price_daily.xls')  # historicalcpi.xls')
    data_xls.to_csv('../data_sets/refined_ny_harbor_price_daily.csv', encoding='utf-8')

    # CSV TO dictionary for insertion to database
    with open('../data_sets/refined_ny_harbor_price_daily.csv') as data_csv:
        reader = csv.reader(data_csv)

        years = []
        for row in reader:
            years.append((row[1], row[2]))
    return years


def create_oil_query(dates, con):

    qry = ''
    for d in dates:
        qry += "INSERT INTO petroleum_prices.refined_oil_price_daily_ny (day, price) VALUES ('{}', {}) ON CONFLICT (day) DO UPDATE SET day=EXCLUDED.day, price=EXCLUDED.price RETURNING day;".format(d[0], d[1])

    return run_query(qry + "select 1;", con)


if __name__ == '__main__':

    # Try to connect to the database, print the exception on failure. Broad exception for broad range of failures.
    try:
        conn = psycopg2.connect("dbname='ds_project_f17' user='postgres' host='localhost' password='{}'".format(ds.db_password))
    except:
        print(sys.exc_info()[0])

    '''
    # Convert the cpi or ppi xls to csv, create the query to insert values (new food_items are inserted
    # while the query is created), and execute the query.
    fd = convert_cpi()
    q = create_cpi_query(fd, conn)
    run_query(q, conn)
    
    # Convert the NAICS code xls to csv, create the query to insert values, and execute the query.
    fd = convert_NAICS(conn)
    
    # Get latitude and longitude from Google Geoservices for each county
    locs = get_locations_query(conn)
    lat_longs = get_lat_long(locs)
    write_lat_longs(lat_longs, conn)
    '''
    # Convert oil prices
    oil = convert_oil()
    oil_q = create_oil_query(oil, conn)

    # Always remember to close your connections when you are finished.
    conn.close()
