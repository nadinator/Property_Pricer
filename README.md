# Property Pricer: a QuAM

### Stefan Baumann and Nadim Bou Alwan 
### 22 April 2020

Our project is a simple query answering machine. Given certain information about a specific piece of property, this machine predicts how much that property would cost to rent (per month in QAR). This can be useful for understanding current market prices, and how to buy/sell property.  

To use this machine, please run the QuAM.ipynb file. Ignore the code and scroll to the bottom to where you will find fields where you must give in the features of your property, and when you are done click "Submit", which you will be given a prediction.

If you want to see how we cleaned the initial data (PF_scrape_final.csv) and determined the best model for our QuAM, please run all the cells starting with DataPreparation.ipynb, then QuAM_report.ipynb. You will see that we produce a new version of our data which is clean (data_clean.csv), which we use for our QuAM.