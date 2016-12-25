import sys,csv
import requests
import urllib2, cookielib
import re
from pymongo import MongoClient
from bs4 import BeautifulSoup
from pprint import pprint
client = MongoClient()
db = client.crawledCompanyBasics
ticker = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','1','2','3','4','5','6','7','8','9']
for increment in ticker:
	current_sitemap_url = 'http://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker='+str(increment)
	hdr = {
		'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
		'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
		'Accept-Encoding': 'none',
		'Accept-Language': 'en-US,en;q=0.8',
		'Connection': 'keep-alive'
	}
	req = urllib2.Request(current_sitemap_url,headers = hdr)
	sitemap_html_page = urllib2.urlopen(req).read()
	soup_url = BeautifulSoup(sitemap_html_page, "html.parser")
	ulObject = soup_url.find("ul", class_="companyList")
	anchorsObject = ulObject.findAll("a")

	for i in anchorsObject:
		url = "http://economictimes.indiatimes.com"+i.get('href')
		companyDetailsArray = i.get('href').split("/")
		companyLinkName = companyDetailsArray[1]
		companyIdArray = companyDetailsArray[3].split(".")
		companyId = companyIdArray[0].split('-')
		res = {
			"url": url,
			"companyLinkName": companyLinkName,
			"companyId": companyId[1]
		}

		result = db.companyCollection.insert_one(
			{
				"url": url,
				"companyLinkName": companyLinkName,
				"companyId": companyId[1]
			}
		)

		print res
	# loc_obj = soup_url.findAll(attrs={"data-fb-result":True})
	# actual_url_to_crawl = []
	# for i in loc_obj:
	# 	actual_url_to_crawl.append(i.get('data-fb-result'))
	# #urls = ['http://www.skysports.com/football/teams/chelsea']
	# for x in actual_url_to_crawl:
	# 	r = requests.get(x, allow_redirects = True).text
	# 	soup = BeautifulSoup(r, "html.parser")
		
	# 	for script in soup(["script", "style"]):
	# 		script.extract()    # rip it out
	# 	#meta_description_obj = soup.findAll(attrs={"name":"description"})
	# 	#meta_description = meta_description_obj[0]['content'].encode('utf-8').replace('  ','~').replace('~','')
	# 	body_check = soup.body
	# 	title_check = soup.title
	# 	if (body_check is None or title_check is None):
	# 		continue;
	# 	body = body_check.get_text(' ','<br/>').encode('utf-8').replace('\n',' ').replace('  ','~').replace('~','')
	# 	title = soup.title.get_text().encode('utf-8').replace('  ','~').replace('~','')
	# 	result = db.qmulCrawledCollection.insert_one(
	# 		{
	# 			"url": x,
	# 			"body": body,
	# 			"title": title
	# 		}
	# 	)
	# 	print(body)
	# 	print "------------------------------------------------------------------------------------"
	# 	print "------------------------------------------------------------------------------------"
	# 	print "------------------------------------------------------------------------------------"
