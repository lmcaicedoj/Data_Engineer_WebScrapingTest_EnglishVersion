# (i) Libraries
import requests
from bs4 import BeautifulSoup
import re
import time
from multiprocessing import Pool
import pandas as pd
import numpy as np

import sys
sys.setrecursionlimit(100000)


# (ii) Website references:
URL1 = "https://books.toscrape.com/catalogue/page-"
title_ref = []
rating_final = []
a = 25
for page in range (1,a):
    req = requests.get(URL1+str(page)+'.html')
    soup = BeautifulSoup(req.text, "html.parser")
    info_li = soup.find_all('li', attrs={'class': 'col-xs-6 col-sm-4 col-md-3 col-lg-3'})
    len(info_li)
    if page < a:
       titles_ini = []
              
       for i in range(0,len(info_li)):
           title = soup.find_all('li', class_= 'col-xs-6 col-sm-4 col-md-3 col-lg-3')[i].find("article", class_="product_pod").find("h3").find("a")
           titles_ini.append(title)
           
       for item in titles_ini:
            title_ref.append(item['href'])

# (iii) Complete list with books links:
URL2 = "https://books.toscrape.com/catalogue/"
API_ = []
books_links = []
for t in title_ref:
    API_ = '{}{}'.format(URL2,t)
    books_links.append(API_)

# (iv) Main function for parsing and scraping each website:
def parse(URL):
                
    # Request and parsing the URL:      
    req2 = requests.get(URL)
    soup2 = BeautifulSoup(req2.text, "html.parser")
    
    # Name Scraping:
    Name = soup2.find('div', class_ = 'col-sm-6 product_main')
    text = list(Name.descendants)
    Name_final = text[2]
    
    # Rating Scraping:
    rating = soup2.find('div', class_= 'col-sm-6 product_main').find("p").find_next("p").find_next("p", class_= True)
    temp = rating['class']
    Rating_final = temp[1]
    if Rating_final == 'One':
        Rating_final = Rating_final.replace('One', '1')
    elif Rating_final == 'Two':
        Rating_final = Rating_final.replace('Two', '2')
    elif Rating_final ==  'Three':
        Rating_final = Rating_final.replace('Three', '3')   
    elif Rating_final ==  'Four':
        Rating_final = Rating_final.replace('Four', '4') 
    else:
        Rating_final = Rating_final.replace('Five', '5')  
    
    Rating_final = int(Rating_final)  
    
    
    # UPC, Availability and Review Scraping:
    UPC_Avai_Review_Price = soup2.find('table', class_ = 'table table-striped')
    text = list(UPC_Avai_Review_Price.descendants)
    UPC_final = text[6] 
    Avai_final = int((re.findall(r'\d+', text[47]))[0])
    Review_final = int(re.findall(r'\d+',text[56])[0])
    Price_final = float(re.findall(r'\d+',text[22])[0])
    
    # Category Scraping:
    category = soup2.find('ul', class_ = 'breadcrumb')#.findAll('li')#.find("a", href = True)
    text = list(category.descendants)
    Category_final= text[16]
    
    return Name_final, Rating_final, Price_final, UPC_final, Avai_final, Review_final, Category_final


def parse_with_mp(URL):
    start_time = time.time()
    p = Pool()
    result = p.map(parse, URL) # Name_final, Rating_final, Price_final, UPC_final, Avai_final, Review_final, Category_final = p.map(parse, URL)
    p.close()
    p.join()
    
    # data ={'Name': Name_final, 'Rating': Rating_final, 'Price (£)': Price_final,
    # 'UPC': UPC_final, '# Avilable': Avai_final, '# of Review': Review_final, 'Category':Category_final}
    # DF =  pd.DataFrame(data) 
    end_time = time.time() - start_time
    print(f"Processing This {len(URL)} URLs took {end_time} seconds using multiprocessing") 
    
def parse_no_mp (URL):
    start_time = time.time()
    
    # Main varibles
    Name_final = []
    Rating_final = []
    Price_final = []
    UPC_final = []
    Avai_final = []
    Review_final =[]
    Category_final = []
    
    for i in URL:
            # Request and parsing the URL:      
            req2 = requests.get(i)
            soup2 = BeautifulSoup(req2.text, "html.parser")
            
            # Name Scraping:
            Name = soup2.find('div', class_ = 'col-sm-6 product_main')
            text = list(Name.descendants)
            Name_final.append(text[2])
            
            # Rating Scraping:
            rating = soup2.find('div', class_= 'col-sm-6 product_main').find("p").find_next("p").find_next("p", class_= True)
            temp = rating['class']
            rating_final = temp[1]
            if rating_final == 'One':
                rating_final = rating_final.replace('One', '1')
            elif rating_final == 'Two':
                rating_final = rating_final.replace('Two', '2')
            elif rating_final ==  'Three':
                rating_final = rating_final.replace('Three', '3')   
            elif rating_final ==  'Four':
                rating_final = rating_final.replace('Four', '4') 
            else:
                rating_final = rating_final.replace('Five', '5')  
            
            Rating_final.append(int(rating_final))  
            
            
            # UPC, Availability and Review Scraping:
            UPC_Avai_Review_Price = soup2.find('table', class_ = 'table table-striped')
            text = list(UPC_Avai_Review_Price.descendants)
            UPC_final.append(text[6]) 
            Avai_final.append(int((re.findall(r'\d+', text[47]))[0]))
            Review_final.append(int(re.findall(r'\d+',text[56])[0]))
            Price_final.append(float(re.findall(r'\d+',text[22])[0]))
            
            # Category Scraping:
            category = soup2.find('ul', class_ = 'breadcrumb')#.findAll('li')#.find("a", href = True)
            text = list(category.descendants)
            Category_final.append(text[16])
     
    # data ={'Name': Name_final, 'Rating': Rating_final, 'Price (£)': Price_final,
    # 'UPC': UPC_final, '# Avilable': Avai_final, '# of Review': Review_final, 'Category':Category_final}
    # DF =  pd.DataFrame(data) 
        
    end_time = time.time() - start_time
    print(f"Processing {len(URL)} URLs took {end_time} seconds using serial processing") 
    


if __name__ == '__main__':
    
    URL = books_links
    parse_no_mp(URL)
    parse_with_mp(URL)

# Test results in Git-Bash terminal:
    
# Test 1: 
# $  python3 multiprocessing_pool_books_case.py
# Processing 40 URLs took 17.313687324523926 seconds using serial processing
# Processing This 40 URLs took 5.772939682006836 seconds using multiprocessing

# Test 2:
# $ python3 multiprocessing_pool_books_case.py
# Processing 480 URLs took 195.00378012657166 seconds using serial processing
# Processing This 480 URLs took 43.14463472366333 seconds using multiprocessing

# Test 3:    
# $ python3 multiprocessing_pool_books_case.py 
# Processing 1000 URLs took 399.3409550189972 seconds using serial processing
# Processing This 1000 URLs took 87.99348568916321 seconds using multiprocessing

