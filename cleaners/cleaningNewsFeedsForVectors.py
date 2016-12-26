import csv
import re
from pymongo import MongoClient
import nltk
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
readFile=csv.reader(open('../../backupData/RedittNews8Years/RedditNews.csv', 'rb'), delimiter=',')
writeFile=csv.writer(open('../../backupData/RedittNews8Years/RedditCleanedNewsForVectors.csv', 'wb'), delimiter=',')
writeText = open('../../backupData/RedittNews8Years/RedditCleanedNewsForVectors.txt','wb')
checkSet = set()
inc = 0

def textToWords(textToConvert):
	soup = BeautifulSoup(textToConvert, "html5lib")
	sentence = soup.get_text().replace("b\'",'')
	words = re.sub("[^a-zA-Z]"," ", soup.get_text()).replace('\n', '').lower().split()
	finalWords = [w for w in words if not w in stopwords.words("english")]
	return(" ".join(finalWords))


#For RedditNews8Years
readFile=csv.reader(open('../../backupData/RedittNews8Years/RedditNews.csv', 'rb'), delimiter=',')
for row in readFile:
	if(inc==0):
		print row
		inc = inc+1
		continue
	
	cleanedData = textToWords(str(row[1]))
	if cleanedData:
		print cleanedData
		writeFile.writerow(cleanedData)
		writeText.write(cleanedData+'\n')
	inc = inc+1

#For crawledNewsFeeds
writeFile=csv.writer(open('../../backupData/crawledProcessedData/newsFeedsForVectorFormation.csv', 'wb'), delimiter=',')
writeText = open('../../backupData/crawledProcessedData/newsFeedsForVectorFormation.txt','wb')
client1 = MongoClient()
db = client1.crawledCompanyBasics
train = db.actualCrawledNewsFeeds.find({},{'extractedPostBody':1})
inc = 0

for row in train:
	if (inc%100 == 0):
		print inc
	cleanedData = textToWords(row['extractedPostBody'])
	if cleanedData:
		writeFile.writerow(cleanedData)
		writeText.write(cleanedData+'\n')
	inc = inc+1


# for row in readFile:
# 	if(inc==0):
# 		print row
# 		inc = inc+1
# 		continue
# 	check = str(row[1])+str(row[2])+str(row[4]).replace('\n','')
# 	if row[4]:
# 		row[4] = str(row[4]).replace('\n','')
# 		if check not in checkSet:
# 			writeFile.writerow(row)
# 			writeText.write(row[4]+'\n')
# 			checkSet.add( check )
# 	inc = inc+1