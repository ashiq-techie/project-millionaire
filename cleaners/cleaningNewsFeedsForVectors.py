import csv
readFile=csv.reader(open('output.csv', 'rb'), delimiter=',')
writeFile=csv.writer(open('outputWithoutDuplicates.csv', 'wb'), delimiter=',')
writeText = open('outputWithoutDuplicates.txt','wb')
checkSet = set()
inc = 0
for row in readFile:
	if(inc==0):
		print row
		inc = inc+1
		continue
	check = str(row[1])+str(row[2])+str(row[4]).replace('\n','')
	if row[4]:
		row[4] = str(row[4]).replace('\n','')
		if check not in checkSet:
			writeFile.writerow(row)
			writeText.write(row[4]+'\n')
			checkSet.add( check )
	inc = inc+1