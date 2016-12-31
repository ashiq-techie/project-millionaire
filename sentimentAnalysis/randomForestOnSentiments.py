
import pandas as pd
import re,sys,math
from gensim.models import Word2Vec
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np  # Make sure that numpy is imported


test = pd.read_csv("../../backupData/crawledProcessedData/check.csv", header=0, delimiter=",", quotechar="\"")
train = pd.read_csv("../../backupData/crawledProcessedData/completeQuoteArticleMappingPart4New.csv", header=0, delimiter=",", quotechar="\"")
train.columns = list(test.columns.values)
print "loading model"
for df in [train,test]:
	for x in ['volume','close','open','adj_close','high']:
		print x
		df['diff_prev_'+str(x)] = df['prev_'+str(x)]-df[x]
		print df[x].dtype
		print df['next_'+str(x)].astype(int)
		df['diff_next_'+str(x)] = df[x]-df['next_'+str(x)] #this is what we need to predict in regression

# classify good or bad stock
def classifyStock(row, what):
	if row[what] <= 0 :
		return 0
	else:
		return 1

df['good_or_bad_previous_day'] = df.apply (lambda row: classifyStock('diff_prev_close'),axis=1)
df['good_or_bad_next_day'] = df.apply (lambda row: classifyStock('diff_prev_close'),axis=1) #this is what we need to predict in classification


#Random Forest prediction begins for prediction of sentiments

print "Processing Train set"
targetArrayTrain = train.as_matrix(columns=['very_neg','neg','neutral','pos','very_pos'])
estimatorArrayTrain = train.as_matrix(columns=['company_id','diff_prev_volume','diff_prev_close','diff_prev_adj_close','diff_prev_high','diff_prev_open','good_or_bad_previous_day'])
trainDataVecs = pd.read_csv("../../backupData/models/learnedModels/trainDataVecsFromGoogle.csv", header=0, delimiter=",", quotechar="\"").as_matrix()
output = pd.DataFrame(data = np.concatenate((trainDataVecs, estimatorArrayTrain),axis=1))
output.to_csv( "../../backupData/models/learnedModels/randomForest/randomForestSentimentTrain.csv", index=False )

print "Processing Test set"
targetArrayTest = test.as_matrix(columns=['very_neg','neg','neutral','pos','very_pos'])
estimatorArrayTest = test.as_matrix(columns=['company_id','diff_prev_volume','diff_prev_close','diff_prev_adj_close','diff_prev_high','diff_prev_open','good_or_bad_previous_day'])
testDataVecs = pd.read_csv("../../backupData/models/learnedModels/testDataVecsFromGoogle.csv", header=0, delimiter=",", quotechar="\"").as_matrix()
output = pd.DataFrame(data = np.concatenate((testDataVecs, estimatorArrayTest),axis=1))
output.to_csv( "../../backupData/models/learnedModels/randomForest/randomForestSentimentTest.csv", index=False )


from sklearn.ensemble import RandomForestRegressor
forest = RandomForestRegressor( n_estimators = 100 )
# very_neg,neg,neutral,pos,very_pos
print "Fitting a random forest to labeled training data..."
forest = forest.fit( np.concatenate((trainDataVecs, estimatorArrayTrain), axis=1), targetArrayTrain  )


print "predicting sentiments"
targetArrayTestPrediction = forest.predict( np.concatenate((testDataVecs,estimatorArrayTest), axis=1))
output = pd.DataFrame(data = targetArrayTestPrediction)
output.to_csv( "../../backupData/models/learnedModels/randomForest/randomForestSentimentPrediction.csv", index=False )
# stock market prediction


