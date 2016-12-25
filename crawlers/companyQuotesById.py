import sys,csv,json, pymysql.cursors
import requests
import urllib2, cookielib
import pymysql.cursors
import pymysql
import re
from pymongo import MongoClient
from bs4 import BeautifulSoup
from pprint import pprint
client = MongoClient()
db = client.crawledCompanyBasics

connection = pymysql.connect(host="localhost", user="root", password="", db="company_quotes")

for exchangeid in [47,50]:
	for cid in db.companyCollection.find({},{"companyId":1}):
		companyId = cid['companyId']
		# current_sitemap_url = 'http://etfeeds.indiatimes.com/ETServiceChartCompanyPage/GetCompanyPriceInformation?companyid='+companyId+'&exchangeid=50&datatype=eod&filtertype=eod&tagId=9175&firstreceivedataid=2016-11-20&lastreceivedataid=&directions=back&callback=serviceHit.chartResultCallback&scripcodetype=company&uptodataid=2015-11-4&_=1479675215098'
		current_sitemap_url = 'http://etfeeds.indiatimes.com/ETServiceChartCompanyPage/GetCompanyPriceInformation?companyid='+companyId+'&exchangeid='+str(exchangeid)+'&datatype=eod&filtertype=eod&tagId=9175&firstreceivedataid=2016-11-20&lastreceivedataid=&directions=back&callback=serviceHit.chartResultCallback&scripcodetype=company&uptodataid=2012-11-4&_=1479675215098'
		hdr = {
			'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
			'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
			'Accept-Encoding': 'none',
			'Accept-Language': 'en-US,en;q=0.8',
			'Connection': 'keep-alive'
		}
		try:
			req = urllib2.Request(current_sitemap_url,headers = hdr)
			jsonResponse = urllib2.urlopen(req).read()
			#31
			removeCallBack = jsonResponse[31:]
			jsonResponse = removeCallBack[:-1]
			# print jsonResponse
			data = json.loads(jsonResponse)
			

			industrycscripcode = str(data['query']['results']['peerdata']['industrycscripcode']) if 'industrycscripcode' in data['query']['results']['peerdata'] else "" 
			indexidcscripcode = str(data['query']['results']['peerdata']['indexidcscripcode']) if 'indexidcscripcode' in data['query']['results']['peerdata'] else "" 
			sectorcscripcode = str(data['query']['results']['peerdata']['sectorcscripcode']) if 'sectorcscripcode' in data['query']['results']['peerdata'] else "" 
			sectorindexcscripcode = str(data['query']['results']['peerdata']['sectorindexcscripcode']) if 'sectorindexcscripcode' in data['query']['results']['peerdata'] else "" 
			for x in data['query']['results']['quote']:
				print "------------------------------------------------------------------------------------"
				print type(industrycscripcode)
				print "------------------------------------------------------------------------------------"
				print x
				with connection.cursor() as cursor:
					# Create a new record
					sql = "INSERT INTO `company_quotes_by_date` (`company_id`, `low`, `open`, `adj_close`, `close`, `date`, `volume`, `high`, `industrycscripcode`, `sectorindexcscripcode`, `sectorcscripcode`, `indexidcscripcode`, `exchangeid`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
					cursor.execute(sql, (companyId, x['Low'], x['Open'], x['Adj_Close'], x['Close'], x['Date'], x['Volume'], x['High'], industrycscripcode, sectorindexcscripcode, sectorcscripcode, indexidcscripcode, exchangeid))
					# print data['query']['results']['quote'][0]['Date']
			connection.commit()
		except Exception as e:
			error = "Unexpected error: "+ str(e)
			print error
			result = db.errorCompanyQuotesToCrawl.insert_one(
				{
					"current_sitemap_url": current_sitemap_url,
					"data": data,
					"error": error,
					"companyId": companyId
				}
			)




			