import sys,csv,json, pymysql.cursors, datetime
import pymysql
from pymongo import MongoClient
import numpy as np
import nltk
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
from pycorenlp import StanfordCoreNLP
import re
import pprint
pp = pprint.PrettyPrinter()

# get all companyIds
client1 = MongoClient()
db = client1.crawledCompanyBasics

newsRecords = db.uniqueCrawledNewsFeeds.find({'postType': {'$in':["News Updates", "Press Releases"]}},{'_id':1,'companyId':1, 'cleanedDate':1,'extractedPostBody':1,'postType':1,'linkTitle':1,'linkUrl':1}).skip(45719).limit(70000).batch_size(5)
print newsRecords.count()

writeFile=csv.writer(open('../../backupData/crawledProcessedData/completeQuoteArticleMappingPart45719Above.csv', 'wb'), delimiter=',')


def textToWords(textToConvert):
	soup = BeautifulSoup(textToConvert, "html5lib")
	sentence = soup.get_text().replace("b\'",'')
	words = re.sub("[^a-zA-Z]"," ", soup.get_text()).replace('\n', '').lower().split()
	finalWords = [w for w in words if not w in stopwords.words("english")]
	return(" ".join(finalWords))

iterRecords = 45720


# for each companyIds get quotes with difference 
for news in newsRecords:
	try:	
		print news['companyId']

		connection = pymysql.connect(host="localhost", user="root", password="", db="company_quotes")
		check = True
		with connection.cursor(pymysql.cursors.DictCursor) as cursor:
		
			# Create a new record
			checkIter = 0
			while(check):

				sql = """SELECT DISTINCT
								exchangeid,
							    company_id,
							    low,
							    open,
							    adj_close,
							    close,
							    volume,
							    high,
							    date
							FROM company_quotes_by_date
							WHERE company_id=%s AND date=%s
							ORDER BY exchangeid
							LIMIT 2"""

				cursor.execute(sql,(news['companyId'],news['cleanedDate'].strftime('%Y-%m-%d')))
				# cursor.execute(sql,(cid['companyId']))
				# get all quotes
				quotes = cursor.fetchall()
				print news['cleanedDate']+datetime.timedelta(days=3)
				pp.pprint(quotes)
				if len(quotes)>0:
					check = False
					break
				else:
					if checkIter>4:
						break
					news['cleanedDate'] = news['cleanedDate']+datetime.timedelta(days=1)
					checkIter = checkIter+1

			if checkIter>4 and check:
				db.errorTransformingNewsFeedQuotes.insert({'newsFeedId':news['_id']})
				print "---------------------------------------------------------------------------------------"
				print str(iterRecords) + " records processed"
				print "---------------------------------------------------------------------------------------"
				iterRecords = iterRecords+1
				continue
			
			# find sentiments
			nlp = StanfordCoreNLP('http://localhost:9000')
			outputArray = nlp.annotate(news['extractedPostBody'].encode('ascii','ignore'), properties={
				'annotators': 'tokenize,ssplit,sentiment',
				'outputFormat': 'json'
			})


			sentimentValues = np.zeros(5) #Initialize the sentiment counter
			incr = 0
			for sentence in outputArray['sentences']:
				sentimentValues[sentence['sentimentValue']] = sentimentValues[sentence['sentimentValue']] + 1
				incr = incr+1
			sentimentValues = sentimentValues/incr # gives relative average

			title = textToWords(news['linkTitle'].encode('ascii','ignore'))
			postType = textToWords(news['postType'].encode('ascii','ignore'))
			body = textToWords(news['extractedPostBody'].encode('ascii','ignore'))
			bodyUncleaned = news['extractedPostBody'].encode('ascii','ignore')
			linkUrl = news['linkUrl'].encode('ascii','ignore')

			# for each quote search if there is a news article by date - reduces unwanted quotes
			previousDates = [news['companyId']]
			print "fetching for previous dates"
			for x in range(1,6):
				previousDates.append((news['cleanedDate']-datetime.timedelta(days=x)).strftime('%Y-%m-%d'))
			

		# with connection.cursor(pymysql.cursor.DictCursor) as cursor1:
			# gets u the nearest previous date quote
			sql1= """SELECT DISTINCT
						exchangeid as prev_exchangeid,
					    company_id as prev_company_id,
					    low as prev_low,
					    open as prev_open,
					    adj_close as prev_adj_close,
					    close as prev_close,
					    volume as prev_volume,
					    high as prev_high,
	                    max(date) as prev_date
					FROM company_quotes_by_date
					WHERE company_id=%s AND date IN (%s,%s,%s,%s,%s)
	                GROUP BY exchangeid
	                LIMIT 2
	                """
			cursor.execute(sql1,tuple(previousDates))
			prevQuotes = cursor.fetchall()

			print "fetching for next dates"
			nextDates = [news['companyId']]
			for x in range(1,6):
				nextDates.append((news['cleanedDate']+datetime.timedelta(days=x)).strftime('%Y-%m-%d'))
		
			# gets u the nearest next date quote
			sql2= """SELECT DISTINCT
						exchangeid as next_exchangeid,
					    company_id as next_company_id,
					    low as next_low,
					    open as next_open,
					    adj_close as next_close,
					    close as next_close,
					    volume as next_volume,
					    high as next_high,
	                    min(date) as next_date
					FROM company_quotes_by_date
					WHERE company_id=%s AND date IN (%s,%s,%s,%s,%s)
	                GROUP BY exchangeid
	                LIMIT 2
	                """
			cursor.execute(sql2,tuple(nextDates))
			nextQuotes = cursor.fetchall()
		if not nextQuotes :
			nextQuotes = quotes

		if not prevQuotes :
			prevQuotes = quotes

		print "fetched quotes"
		print "writing to csv"
		
				
		if len(quotes)>1 and len(prevQuotes)>1 and len(nextQuotes)>1:
			for key in range(len(quotes)):
				writeFile.writerow(quotes[key].values()+[title, body, bodyUncleaned, postType, linkUrl]+sentimentValues.tolist()+prevQuotes[key].values()+nextQuotes[key].values())
		else:
			writeFile.writerow(quotes[0].values()+[title, body, bodyUncleaned, postType, linkUrl]+sentimentValues.tolist()+prevQuotes[0].values()+nextQuotes[0].values())
			
		if len(quotes) == 1:
			writeFile.writerow(quotes[0].values()+[title, body, bodyUncleaned, postType, linkUrl]+sentimentValues.tolist()+prevQuotes[0].values()+nextQuotes[0].values())
		

		print "---------------------------------------------------------------------------------------"
		print str(iterRecords) + " records processed"
		print "---------------------------------------------------------------------------------------"
		iterRecords = iterRecords+1
	except:
		db.errorTransformingNewsFeedQuotes.insert({'newsFeedId':news['_id']})
		continue
	