
stockForest = RandomForestRegressor(n_estimators=100)
print "fitting sentiments to forest"
stockForest = stockForest.fit(np.concatenate((np.concatenate((trainDataVecs,estimatorArrayTrain),axis=1),targetArrayTrain),axis=1),train.next_close.values)
print "predicting next day close values with RF predicted sentiments"
nextClosePrediction = stockForest.predict(np.concatenate((np.concatenate((testDataVecs,estimatorArrayTest),axis=1),targetArrayTestPrediction),axis=1))
print "predicting next day close values with stanford predicted sentiments"
nextCloseStanford = stockForest.predict(np.concatenate((np.concatenate((testDataVecs,estimatorArrayTest),axis=1),targetArrayTest),axis=1))


output = pd.DataFrame( data=test.next_close.values )
output.to_csv( "../../backupData/models/learnedModels/randomForestOutputActual.csv", index=False )
output = pd.DataFrame( data=nextClosePrediction )
output.to_csv( "../../backupData/models/learnedModels/randomForestOutputSentiment.csv", index=False )
output = pd.DataFrame( data=nextCloseStanford )
output.to_csv( "../../backupData/models/learnedModels/randomForestOutputStanford.csv", index=False )