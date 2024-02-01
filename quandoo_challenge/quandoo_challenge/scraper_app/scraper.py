import os
import requests
from bs4 import BeautifulSoup
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, insert, text
import threading
import datetime   

#
# scraper.py - Conor OReilly 
# entry point is at the bottom
#


#  Small but powerful function to yield successive n-sized chunks from a list lst
def chunks(lst, n):

    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Little function just to test the location added to the .env file
def test_url(location):

    url = "https://www.quandoo.de/en/result?destination=" + location

    response = requests.get(url)

    if response.status_code == 200:
        return True
    else:
        return False

# This is the main function of the script, its calls the functions to scrape the website and insert the data to the DB 
def getData(location):

    # Test the location name, if it doesnt work, just use berlin as a default
    if not test_url(location):
        print("invalid location, using berlin as default")
        location = "berlin"


    print("Starting to scrape restaurants . . .")

    # Call function to scrape restaurant data
    dfRestauraunts = scrape_restaurants(location)

    # Add a timestamp to it
    dfRestauraunts['uploaded_at'] = datetime.datetime.now()
    # Call function to insert it into the DB
    insertIntoTable(dfRestauraunts, "staging.restaurants")

    # Bit of a bizarre looking section here. essentially, pulling this much menu data one at a time is time-consuming (though it worked)
    # to try and speed things up I define a bunch of list and threads and do it in parallel

    # From the restaurant data we got above, lets make a list of the IDs and pull the menu data with them
    restaurant_list = dfRestauraunts['restaurant_id'].tolist()

    # Split the list into chunks to do it in parallel
    menulist= list(chunks(restaurant_list, 70))

    # Define the lists, that each thread will use
    menus1 = []
    menus2 = []
    menus3 = []
    menus4 = []
    menus5 = []
    menus6 = []
    menus7 = []
    menus8 = []
    menus9 = []
    menus10 = []

    # Set up the threads and start them
    print("Starting to scrape menus . . .")

    t1 = threading.Thread(target=scrape_menu, args=(menulist[0], menus1))
    t2 = threading.Thread(target=scrape_menu, args=(menulist[1], menus2))
    t3 = threading.Thread(target=scrape_menu, args=(menulist[2], menus3))
    t4 = threading.Thread(target=scrape_menu, args=(menulist[3], menus4))
    t5 = threading.Thread(target=scrape_menu, args=(menulist[4], menus5))
    t6 = threading.Thread(target=scrape_menu, args=(menulist[5], menus6))
    t7 = threading.Thread(target=scrape_menu, args=(menulist[6], menus7))
    t8 = threading.Thread(target=scrape_menu, args=(menulist[7], menus8))
    t9 = threading.Thread(target=scrape_menu, args=(menulist[8], menus9))
    t10 = threading.Thread(target=scrape_menu, args=(menulist[9], menus10))

    t1.start() 
    t2.start() 
    t3.start() 
    t4.start() 
    t5.start() 
    t6.start() 
    t7.start() 
    t8.start() 
    t9.start() 
    t10.start() 

    # Use join for the thread to wait for each other when done
    t1.join() 
    t2.join() 
    t3.join() 
    t4.join() 
    t5.join() 
    t6.join() 
    t7.join() 
    t8.join() 
    t9.join() 
    t10.join() 

    # Combine the menu data together
    menus = menus1 + menus2 + menus3 + menus4 + menus5 + menus6 + menus7 + menus8 + menus9 + menus10
    
    # construct dataframe, add a timestamp and call insertion function
    dfmenues = pd.DataFrame(menus, columns =['restaurant_id', 'menu_item_name', 'menu_item_desc','menu_item_price'])
    dfmenues['uploaded_at'] = datetime.datetime.now()
    insertIntoTable(dfmenues, "staging.menu")

def insertIntoTable(df, table):

        # define connection up here for now (hardcoding it, instead of securing for simplicitys sake)
        pg_user = 'myuser'
        pg_pass= 'mypassword'
        pg_host = 'database'
        pg_port = '5432'
        pg_db ='mydatabase'

        conn = psycopg2.connect("dbname='{db}' user='{user}' host='{host}' port='{port}' password='{passwd}'".format(
                    user=pg_user,
                    passwd=pg_pass,
                    host=pg_host,
                    port=pg_port,
                    db=pg_db))
        cur = conn.cursor() 

        # Create a list of tupples from the dataframe values
        tuples = list(set([tuple(x) for x in df.to_numpy()]))
    
        # Create a comma-separated dataframe columns
        cols = ','.join(list(df.columns))

        print("coulumns : ")
        print(cols)

        # Choose correct insert statement depending whether we're passing restaurant or menu data
        if table == "staging.restaurants" :
            print("creating insertion for restaurant data")
            query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (
                table, cols)
        else:
            print("creating insertion for menu data")
            query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (
                table, cols)

        # Execute the query and commit
        try:
            print("inserting . . .")
            cur.executemany(query, tuples)
            conn.commit()
            print("commited")

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1

# this function takes the location and uses it build a URL and scrape restaurant data, returning a dataframe      
def scrape_restaurants(location):

    base_url = "https://www.quandoo.de/en/result?destination="+location+"&page="

    # We'll loop until there are no more pages of data
    finished = False

    data = []

    count = 1
    while not finished:
        
        # Build URL and send request
        request_url = base_url+str(count)
        response = requests.get(request_url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            print("getting data for " + request_url)

            # Get the url returned to signify the last page of results (calling for a page 22 will just return page 21)
            response_url = response.url
            if request_url == response_url or count == 1 :

                # Get list of restauraunts
                restaurant_entries = soup.find_all("div", class_="sc-AxirZ sc-AxiKw sc-1vnptfs-0 kvjosH")
                
                for entry in restaurant_entries:
                    restaurant_id = entry.find("a", class_= "zt41a1-0 bFQofF").get("href").rpartition('/')[-1]
                    restaurant_name = entry.find("h3", class_= "sc-1vnptfs-3 jjhqNf").get_text()
                    restaurant_area = entry.find("span", class_= "sc-13j8xb1-0 cFfDvf").get_text()
                    restaurant_cuisine = entry.find("span", class_= "sc-1ohzhdx-0 kPKdEs").get_text()

                    # try/except these, some restaurants have no ratings or reviews
                    try:
                        rating = entry.find("div", class_= "sc-1n6pbmb-2 WkOwr").get_text()
                    except:
                        rating = None
                    try:
                        reviews = entry.find("span", class_= "sc-1n6pbmb-4 esfZCj").get_text()
                    except:
                        reviews = None

                    row = [restaurant_id,restaurant_name,restaurant_area,restaurant_cuisine,rating,reviews]
                    # Add row to list
                    data.append(row)
            else:
                finished = True

        count +=1

    # Create and return the dataframe of restaurant data
    df = pd.DataFrame(data, columns=['restaurant_id', 'restaurant_name','restaurant_area','restaurant_cuisine','rating','reviews'])

    return df

# similar to scrape_restaurants except we"re passing in the list of restaurant IDs and a menus list which will be combined at the end of each thread
def scrape_menu(restaurant_ids,menus):

    menus 

    for restaurant_id in restaurant_ids:

        print("strarting to scrape menu : " + restaurant_id)
        response = requests.get("https://www.quandoo.de/en/place/" +restaurant_id+"/menu")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            print("getting data for " + "https://www.quandoo.de/en/place/" +restaurant_id+"/menu")

            # get list of menus, two different menu divs in the website
            menu = soup.find_all("div", class_="sc-AxirZ sc-AxiKw hYsdCq")
            menu2 = soup.find_all("div", class_="sc-AxirZ sc-AxiKw hjiHNa")
            
            # get each menu entry and add them to the menu list
            for entry in menu:
                
                menu_item_name = entry.find("h5", class_= "sc-AxjAm jpCPaA").get_text()

                try:
                    menu_item_price = entry.find("span", class_= "sc-AxjAm gsSxXY").get_text()
                except:
                    menu_item_price = None
                
                try:
                    menu_item_desc = entry.find("p", class_= "sc-AxjAm aJhEQ").get_text()
                except:
                    menu_item_desc = None


                row = [restaurant_id,menu_item_name,menu_item_desc,menu_item_price]
                menus.append(row)

            for entry in menu2:
                
                menu_item_name = entry.find("h5", class_= "sc-AxjAm jpCPaA").get_text()
                
                try:
                    menu_item_price = entry.find("span", class_= "sc-AxjAm gsSxXY").get_text()
                except:
                    menu_item_price = None
                
                try:
                    menu_item_desc = entry.find("p", class_= "sc-AxjAm aJhEQ").get_text()
                except:
                    menu_item_desc = None

                row = [restaurant_id,menu_item_name,menu_item_desc,menu_item_price]
                menus.append(row)

# entry point - run getData with location env variable
if __name__ == "__main__":
    getData(os.getenv('LOCATION'))
