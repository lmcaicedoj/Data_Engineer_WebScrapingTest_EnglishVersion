Hello!
This is the readme file for the Data-Engineer Assessment process for the company ARCH Energy Partners.

The files that compose this assessments are:

(i) Data Engineer Assessment.docx : It has all the instructions and tasks to develop.

(ii) Data Engineer Criteria.docx: It has the questions regarding experiences and profile. 

(iii) Data_Engineer_Assesment_ARCH_LMCJ.ipynb : In this jupyter notebook I was able to explore 
      The website books.toscrape.com and engage with the different tasks proposed in the first file (i).
      This file has 5 different sections:
      (1) Importing libraries;
      (2) Webscrapping: (2.1) Scraping only one page containing 20 books, and (2.2) Scraping all 50 webpages;
      (3) Data Frame (A brief look into the DF variables and its statistcial description) ;
      (4) Multiprocessing (an exploration to what is done later in file (v)); 
      (5) SQLITE (here the DF was loaded to the SQLITE database and the proposed query was solved).
      

(iv) DF.csv: it is DataFrame extracted from the jupyter notebook with the complete dataset
     from the proposed website (1000 books).

(v) multiprocessing_pool_books_case.py : this python file was run from the terminal with the idea to finish off
    faster all the scraping process.
    Three tests were run:
    # Test 1: 
    # $  python3 multiprocessing_pool_books_case.py
    # Processing 40 URLs took 17.313687324523926 seconds using serial processing
    # Processing This 40 URLs took 5.772939682006836 seconds using multiprocessing
    time reduction: 66.66% approximately.


    # Test 2:
    # $ python3 multiprocessing_pool_books_case.py
    # Processing 480 URLs took 195.00378012657166 seconds using serial processing
    # Processing This 480 URLs took 43.14463472366333 seconds using multiprocessing
    time reduction: 77.88% approximately.

    # Test 3:    
    # $ python3 multiprocessing_pool_books_case.py 
    # Processing 1000 URLs took 399.3409550189972 seconds using serial processing
    # Processing This 1000 URLs took 87.99348568916321 seconds using multiprocessing
    time reduction: 79.96% approximately.
    
    Three main observations:
      (1) Multiprocessing is much better for webscraping than serial processing in terms of processing time.
      (2) When the number of URL increase the multiprocessing becomes more efficient (in terms of time usage).
      (3) Multiprocessing improves its performance faster when moving from 40 URLs to 480 URLs. However, when URLs become a relatively 
          large number (1000) it seems to reach a plateau for processing time reduction (close to 80% approximately).   