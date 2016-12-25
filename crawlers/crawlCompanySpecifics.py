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
	urlsToCrawl = []
	for i in anchorsObject:
		url = "http://economictimes.indiatimes.com"+i.get('href')
		urlsToCrawl.append(url)

	for i in urlsToCrawl:
		