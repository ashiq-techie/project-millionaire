import sys,csv,json, pymysql.cursors
import requests
import urllib2, cookielib
import re
from pymongo import MongoClient
from bs4 import BeautifulSoup
from pprint import pprint
client1 = MongoClient()
db = client1.crawledCompanyBasics
companyIds = db.companyCollection.find({},{"companyId":1}).skip(4000)
client1.close()
print companyIds.count()
for cid in companyIds:
	client = MongoClient()
	db = client.crawledCompanyBasics
	try:
		companyId = cid['companyId']
		# current_sitemap_url = 'http://etfeeds.indiatimes.com/ETServiceChartCompanyPage/GetCompanyPriceInformation?companyid='+companyId+'&exchangeid=50&datatype=eod&filtertype=eod&tagId=9175&firstreceivedataid=2016-11-20&lastreceivedataid=&directions=back&callback=serviceHit.chartResultCallback&scripcodetype=company&uptodataid=2015-11-4&_=1479675215098'
		current_sitemap_url = 'http://etspeedapi.indiatimes.com/etspeeds/search.ep?outputtype=json&tag=0&authorid=&latest=true&mostpopular=false&mostcomment=false&mostshared=false&category=Analyst%20Posts,Journalist%20Posts,Community,News%20Updates,Press%20Releases&expertid=&lastpostid=&pageno=1&pagesize=1&companyid='+str(companyId)+'&loggedinuserid=&postid=&fav=true&_=1479858792731'
		hdr = {
			'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
			'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
			'Accept-Encoding': 'none',
			'Accept-Language': 'en-US,en;q=0.8',
			'Connection': 'keep-alive'
		}
		# try:
		req = urllib2.Request(current_sitemap_url,headers = hdr)
		jsonResponse = urllib2.urlopen(req).read()
		#31
		removeCallBack = jsonResponse[5:]
		jsonResponse = removeCallBack[:-1]
		# print jsonResponse
		data = json.loads(jsonResponse)
		
		size = data['summary'][0]['size']
		print size
		 # use the size to scrape everything

		new_sitemap_url = 'http://etspeedapi.indiatimes.com/etspeeds/search.ep?outputtype=json&tag=0&authorid=&latest=true&mostpopular=false&mostcomment=false&mostshared=false&category=Analyst%20Posts,Journalist%20Posts,Community,News%20Updates,Press%20Releases&expertid=&lastpostid=&pageno=1&pagesize='+str(size)+'&companyid='+str(companyId)+'&loggedinuserid=&postid=&fav=true&_=1479858792731'
		req = urllib2.Request(new_sitemap_url,headers = hdr)
		jsonResponse = urllib2.urlopen(req).read()
		#31
		removeCallBack = jsonResponse[5:]
		jsonResponse = removeCallBack[:-1]
		ustr_to_load = unicode(jsonResponse, 'latin-1')
		print ustr_to_load[:62]
		completeData = json.loads(ustr_to_load)
		size = completeData['summary'][0]['size']
		
		for res in completeData['res']:
			createdDate = res['createdDate']
			effectiveDate = res['effectivedate']
			postType = res['posttype']
			print postType
			
			if postType == 'Community':
				print "check"
				continue
			elif postType == 'News Updates':
				try:
					linkUrl = res['linkUrl']
					displayname = res['displaynamelower']
					req = urllib2.Request(linkUrl,headers = hdr)
					html = urllib2.urlopen(req).read()
					soup = BeautifulSoup(html, "html.parser")
					#remove unnecessary things (scripts, styles, ...)
					for script in soup("script"):
					    soup.script.extract()

					for style in soup("style"):
					    soup.style.extract()
					if displayname == 'ndtvprofit':				
						divObject = soup.find("div", class_="ins_descp")
						desc = divObject.text.encode('utf-8').replace('\n',' ')
						linkTitle = res['linkTitle']+' '+desc
						postBody = soup.find("span", class_="ins_storybody").text.encode('utf-8').replace('\n', ' ')
					elif displayname == 'financialxpress':
						divObject = soup.find("div", class_="storybox")
						title = divObject.find('h1').text.encode('utf-8').replace('\n', ' ')
						desc = divObject.find('h2', class_="synopsis").text.encode('utf-8').replace('\n', ' ')
						postBody = divObject.text.encode('utf-8').replace('\n', ' ')
						linkTitle = title+' '+desc
					elif displayname == "livemint":
						title = soup.find('h1').text.encode('utf-8').replace('\n', ' ')
						desc = soup.find('p', class_='intro-box').text.encode('utf-8').replace('\n', ' ')
						postBody = soup.find("div", class_="story-content").text.encode('utf-8').replace('\n', ' ')
						linkTitle = title+' '+desc
					elif displayname == "businessline":
						title = soup.find('h1').text.encode('utf-8').replace('\n', ' ')
						postBody = soup.find("div", class_="article-text ").text.encode('utf-8').replace('\n', ' ')
						linkTitle = title
					elif displayname == "businessstandard":
						title = soup.find('h1', class_="headline").text.encode('utf-8').replace('\n', ' ')
						desc = soup.find('h2', class_='alternativeHeadline').text.encode('utf-8').replace('\n', ' ')
						postBody = soup.find("span", class_="p-content").text.encode('utf-8').replace('\n', ' ')
						linkTitle = title+' '+desc
					else :
						continue
					
					print "_________________________---"
					result = db.crawledNewsFeedsCollectionNewFour.insert_one(
						{
							"companyId": companyId,
							"createdDate":createdDate,
							"effectiveDate":effectiveDate,
							"postType": postType,
							"linkTitle":linkTitle,
							"linkUrl":linkUrl,
							"extractedPostBody": postBody,
							"sitePostBodyOrg": res['postbodyOrg'].replace('<br/>',' ').encode('utf-8').replace('\n',' ')
						}
					)
				except Exception, e:
					error = "Unexpected error: " + str(e)
					err = db.errorCompanyNewsFeedToCrawl.insert_one(
						{
							"res": res,
							"error": error,
							"companyId": companyId
						}
					)
					continue
				
			elif postType == 'Analyst Posts':
				try:
					print "check"
					try:
						linkUrl = res['linkUrl']
						req = urllib2.Request(linkUrl,headers = hdr)
						html = urllib2.urlopen(req).read()
						soup = BeautifulSoup(html, "html.parser")
						linkTitle = res['linkTitle']
						for script in soup(["script", "style"]):
							script.extract()
						extractedPostBody = soup.body.get_text().encode('utf-8').replace('\n',' ')
					except Exception, e:
						linkUrl = ''
						extractedPostBody = ''
						linkTitle = ''
					print "----------------------"
					result = db.crawledNewsFeedsCollectionNewFour.insert_one(
						{
							"companyId": companyId,
							"createdDate":createdDate,
							"effectiveDate":effectiveDate,
							"postType": postType,
							"linkTitle":res['linkTitle'],
							"linkUrl":linkUrl,
							"extractedPostBody": extractedPostBody,
							"sitePostBodyOrg": res['postbodyOrg'].replace('<br/>',' ').encode('utf-8').replace('\n',' ')
						}
					)
				except Exception, e:
					error = "Unexpected error: " + str(e)
					err = db.errorCompanyNewsFeedToCrawl.insert_one(
						{
							"res": res,
							"error": error,
							"companyId": companyId
						}
					)
					continue

			elif postType == 'Journalist Posts':
				try:
					try:
						linkUrl = res['linkUrl']
						req = urllib2.Request(linkUrl,headers = hdr)
						finalRequest = urllib2.urlopen(req)
						linkFinal = finalRequest.geturl()
						if 'twitter.com/' in linkFinal:
							continue
						html = finalRequest.read()
						soup = BeautifulSoup(html, "html.parser")
						linkTitle = res['linkTitle']
						for script in soup(["script", "style"]):
							script.extract()
						extractedPostBody = soup.body.get_text().encode('utf-8').replace('\n',' ')
					except Exception, e:
						linkUrl = ''
						extractedPostBody = ''
						linkTitle = ''
					print "check";
					result = db.crawledNewsFeedsCollectionNewFour.insert_one(
						{
							"companyId": companyId,
							"createdDate":createdDate,
							"effectiveDate":effectiveDate,
							"postType": postType,
							"linkTitle":linkTitle,
							"linkUrl":linkUrl,
							"extractedPostBody": extractedPostBody,
							"sitePostBodyOrg": extractedPostBody
						}
					)
				except Exception, e:
					error = "Unexpected error: " + str(e)
					err = db.errorCompanyNewsFeedToCrawl.insert_one(
						{
							"res": res,
							"error": error,
							"companyId": companyId
						}
					)
					continue
					
			else : # postType == 'Press Releases'
				try:
					linkUrl = res['linkUrl']
					req = urllib2.Request(linkUrl,headers = hdr)
					html = urllib2.urlopen(req).read()
					soup = BeautifulSoup(html, "html.parser")
					tdObject = soup.find("td", class_="TTRow_leftnotices")
					linkTitle = res['linkTitle']
					if (tdObject is None):
						continue;
					for x in tdObject:
						postBody = tdObject.text.encode('utf-8').replace('\n',' ')
						result = db.crawledNewsFeedsCollectionNewFour.insert_one(
							{
								"companyId": companyId,
								"createdDate":createdDate,
								"effectiveDate":effectiveDate,
								"postType": postType,
								"linkTitle":linkTitle,
								"linkUrl":linkUrl,
								"extractedPostBody": postBody,
								"sitePostBodyOrg": res['postbodyOrg'].replace('<br/>',' ').encode('utf-8').replace('\n',' ')
							}
						)
				except Exception, e:
					error = "Unexpected error: " + str(e)
					err = db.errorCompanyNewsFeedToCrawl.insert_one(
						{
							"res": res,
							"error": error,
							"companyId": companyId
						}
					)
					continue
	except:
		error = "Unexpected error: " + str(e)
		err = db.errorCompanyNewsFeedCompleteFailures.insert_one(
			{
				"error": error,
				"companyId": companyId,
				"url": current_sitemap_url
			}
		)
		continue

	# except Exception as e:
	# 	error = "Unexpected error: "+ str(e)
	# 	print error
	# 	result = db.errorCompanyQuotesToCrawl.insert_one(
	# 		{
	# 			"current_sitemap_url": current_sitemap_url,
	# 			"data": data,
	# 			"error": error,
	# 			"companyId": companyId
	# 		}
	# 	)
	# 	print "------------------------------------------------------------------------------------------------"
	# 	print "------------------------------------------------------------------------------------------------"
	# 	print "------------------------------------------------------------------------------------------------"
	client.close()
	print str(companyId)+' done'
	print "------------------------------------------------------------------------------------------------"
	print "------------------------------------------------------------------------------------------------"
	print "------------------------------------------------------------------------------------------------"
	print "------------------------------------------------------------------------------------------------"






	