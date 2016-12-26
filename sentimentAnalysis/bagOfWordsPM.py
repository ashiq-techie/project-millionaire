import pandas as pd
import re
import nltk
# nltk.download()
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from pymongo import MongoClient
client1 = MongoClient()
db = client1.crawledCompanyBasics
train = db.actualCrawledNewsFeeds.find({},{'_id':1,'extractedPostBody':1,'linkTitle':1})
del client1
def textToWords(textToConvert):
	soup = BeautifulSoup(textToConvert, "html5lib")
	words = re.sub("[^a-zA-Z]"," ", soup.get_text()).replace('\n', '').lower().split()
	finalWords = [w for w in words if not w in stopwords.words("english")]
	return(" ".join(finalWords))

print "Cleaning and parsing the training set...\n"
inc = 1
for i in train:
	if 'client2' not in globals():
		client2 = MongoClient()
	db = client2.crawledCompanyBasics
	result = db.actualCrawledNewsFeeds.update(
		{"_id":i['_id']},
		{
			'$set':	{
				"cleanedPostBody": textToWords(i['extractedPostBody']),
				"cleanedTitle":textToWords(i['linkTitle'])
			}
		}
	)
	inc = inc+1
	if(inc%1000 == 0):
		del client2
		print inc
	
	
