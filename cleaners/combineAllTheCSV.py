import pandas as pd
import numpy as np
check = pd.read_csv("../../backupData/crawledProcessedData/check.csv", header=0, delimiter=",", quotechar="\"", error_bad_lines=False)
headers = list(check.columns.values)
df1 = pd.read_csv("../../backupData/crawledProcessedData/completeQuoteArticleMappingPart3New.csv", header=0, delimiter=",", quotechar="\"", error_bad_lines=False).as_matrix()
df2 = pd.read_csv("../../backupData/crawledProcessedData/completeQuoteArticleMappingPart4New.csv", header=0, delimiter=",", quotechar="\"", error_bad_lines=False).as_matrix()
df3 = pd.read_csv("../../backupData/crawledProcessedData/completeQuoteArticleMappingPart45719Above.csv", header=0, delimiter=",", quotechar="\"", error_bad_lines=False).as_matrix()

print type(df1)

print df1.shape
print df2.shape
print df3.shape
result = np.concatenate((np.concatenate((df1,df2)),df3))

result = pd.DataFrame(data=result)
del result[9]
del headers[9]
result = result.dropna(axis=0)
print result.shape
print result.isnull().sum()
result.to_csv( "../../backupData/crawledProcessedData/completeQuoteArticleMappingNewComp1.csv",header=headers,index=False )