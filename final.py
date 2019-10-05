#weather = sc.textFile("sample.csv")
weather = sc.textFile("2019.csv")

weatherParse = weather.map(lambda line : line.split(","))

weatherPrecp = weatherParse.filter(lambda x: x[2] == "PRCP")
# x[0] is the station
# x[3] is the precipitation value
weatherPrecpCountByKey = weatherPrecp.map(lambda x : (x[0], (int(x[3]), 1)))

weatherPrecpAddByKey = weatherPrecpCountByKey.reduceByKey(lambda v1,v2 : (v1[0]+v2[0], v1[1]+v2[1]))

weatherAverages = weatherPrecpAddByKey.map(lambda k:(k[0], k[1][0] / float(k[1][1] ) ) )

x = weather.count()

b=weatherAverages.sortBy(lambda x:x[1],ascending=False).take(10)

print("We got ", x," records")
for a in range(10):
	print("Station %s had average precipitations of %f" % (b[a][0],b[a][1]))

