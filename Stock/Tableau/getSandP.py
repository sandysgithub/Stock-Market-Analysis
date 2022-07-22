from datetime import datetime
from concurrent import futures

import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web

def download_stock(stock):
	try:
		print(stock)
		stock_df = web.DataReader(stock,'yahoo', start_time, now_time)
		stock_df['Name'] = stock
		output_name = stock + '_data.csv'
		stock_df.to_csv("./stocks/"+output_name)
	except:
		bad_names.append(stock)
		print('bad: %s' % (stock))

if __name__ == '__main__':

	""" set the download window """
	now_time = datetime.now()
	start_time = datetime(now_time.year - 12, now_time.month , now_time.day)

	""" list of s_anp_p companies """
	csv1 = pd.read_csv("MarketWatch.csv",)
	companies = csv1.iloc[:,1].to_list()
	company_tickers = [x+".NS" for x in companies]	
	bad_names =[] #to keep track of failed queries

	"""here we use the concurrent.futures module's ThreadPoolExecutor
		to speed up the downloads buy doing them in parallel 
		as opposed to sequentially """

	#set the maximum thread number
	max_workers = 50

	workers = min(max_workers, len(company_tickers)) #in case a smaller number of stocks than threads was passed in
	with futures.ThreadPoolExecutor(workers) as executor:
		res = executor.map(download_stock, company_tickers)

	
	""" Save failed queries to a text file to retry """
	if len(bad_names) > 0:
		with open('failed_queries.txt','w') as outfile:
			for name in bad_names:
				outfile.write(name+'\n')
	
	#get market cap for all tickers
	market_cap = web.get_quote_yahoo(company_tickers)['marketCap']
	df = pd.DataFrame({'Name':market_cap.index, 'Market Cap':market_cap.values})
	df.to_csv("marketcap.csv", index=False)