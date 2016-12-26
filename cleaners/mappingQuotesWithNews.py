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
companyIds = db.companyCollection.find({},{"companyId":1})
client1.close()
print companyIds.count()

writeFile=csv.writer(open('../../backupData/crawledProcessedData/completeQuoteArticleMapping.csv', 'wb'), delimiter=',')

def textToWords(textToConvert):
	soup = BeautifulSoup(textToConvert, "html5lib")
	sentence = soup.get_text().replace("b\'",'')
	words = re.sub("[^a-zA-Z]"," ", soup.get_text()).replace('\n', '').lower().split()
	finalWords = [w for w in words if not w in stopwords.words("english")]
	return(" ".join(finalWords))

# for each companyIds get quotes with difference 
for cid in companyIds:
	print "working on " + str(cid['companyId'])
	connection = pymysql.connect(host="localhost", user="root", password="", db="company_quotes")
	with connection.cursor(pymysql.cursors.DictCursor) as cursor:
		# Create a new record
		sql = """SET @kilo_company_id=0;
					SET @kilo_low=0;
					SET @kilo_open=0;
					SET @kilo_adj_close=0;
					SET @kilo_close=0;
					SET @kilo_volume=0;
					SET @kilo_high=0;

					SELECT DISTINCT
						mt1.exchangeid,
					    mt1.company_id - @kilo_company_id AS company_id_since_last_date,
					    mt1.low - @kilo_low AS low_since_last_date,
					    mt1.open - @kilo_open AS open_since_last_date,
					    mt1.adj_close - @kilo_adj_close AS adj_close_since_last_date,
					    mt1.close - @kilo_close AS close_since_last_date,
					    mt1.volume - @kilo_volume AS volume_since_last_date,
					    mt1.high - @kilo_high AS high_since_last_date,
					    @kilo_company_id := mt1.company_id company_id,
					    @kilo_low := mt1.low low,
					    @kilo_open := mt1.open open,
					    @kilo_adj_close := mt1.adj_close adj_close,
					    @kilo_close := mt1.close close,
					    @kilo_volume := mt1.volume volume,
					    @kilo_high := mt1.high high,
						mt1.date
					FROM company_quotes_by_date mt1
					WHERE company_id=%s
					ORDER BY mt1.date"""
		cursor.callproc('find_all',[cid['companyId']])
		# cursor.execute(sql,(cid['companyId']))
		# get all quotes
		quotes = cursor.fetchall()
	print "fetched " + str(len(quotes))+" quotes"
	# for each quote search if there is a news article by date - reduces unwanted quotes
	client2 = MongoClient()
	db = client2.crawledCompanyBasics
	print "checking for articles"
	for quote in quotes:
		result = db.actualCrawledNewsFeeds.find({'companyId':str(quote['company_id']),'postType': {'$in':["News Updates", "Press Releases"]},'cleanedDate':datetime.datetime.combine(quote['date'], datetime.datetime.min.time())},{'extractedPostBody':1,'postType':1,'linkTitle':1})
		if result.count():
			print "fetched " + str(result.count())+" articles"
			print "extracting and writing to csv"
			for record in result:
				nlp = StanfordCoreNLP('http://localhost:9000')
				outputArray = nlp.annotate(record['extractedPostBody'].encode('ascii','ignore'), properties={
					'annotators': 'tokenize,ssplit,sentiment',
					'outputFormat': 'json'
				})
				

				sentimentValues = np.zeros(5) #Initialize the sentiment counter
				incr = 0
				for sentence in outputArray['sentences']:
					sentimentValues[sentence['sentimentValue']] = sentimentValues[sentence['sentimentValue']] + 1
					incr = incr+1
				sentimentValues = sentimentValues/incr # gives relative average

				title = textToWords(record['linkTitle'].encode('ascii','ignore'))
				postType = textToWords(record['postType'].encode('ascii','ignore'))
				body = textToWords(record['extractedPostBody'].encode('ascii','ignore'))
				bodyUncleaned = record['extractedPostBody'].encode('ascii','ignore')

				writeFile.writerow(quote.values()+[title, body, bodyUncleaned, postType]+sentimentValues.tolist())
		else:
			continue
	client2.close()
	print "---------------------------------------------------------------------------------------"
	print "---------------------------------------------------------------------------------------"
	print "---------------------------------------------------------------------------------------"