from pymongo import MongoClient
import datetime
client1 = MongoClient()
db = client1.crawledCompanyBasics
resultSet = db.actualCrawledNewsFeeds.find({},{'_id':1,'createdDate':1})
inc =0
for row in resultSet:

	dateArr = row['createdDate'].split()
	date = [dateArr[1],dateArr[2],dateArr[5]] 
	result = db.actualCrawledNewsFeeds.update(
		{"_id":row['_id']},
		{
			'$set':	{
				"cleanedDate": datetime.datetime.strptime(' '.join(date), '%b %d %Y')
			}
		}
	)
	inc = inc+1
	if(inc%1000 == 0):
		print inc