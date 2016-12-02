import borough_finder
import time

print "one million borough lookups"

start_time = time.time()

for i in range(0,1000000/6):
  #should be Bronx
  if borough_finder.find_borough( 40.854453, -73.854218) != 'The Bronx':
    print "Error - didn't find Bronx on loop %d" % i
    exit()

  #should be Staten island
  if borough_finder.find_borough( 40.591136,-74.132996) != 'Staten Island':
    print "Error - didn't find Staten Island on loop %d" % i
    exit()

  #should be Brooklyn
  if borough_finder.find_borough( 40.629709,-73.936615) != "Brooklyn":
    print "Error - didn't find Brooklyn on loop %d" % i
    exit()

  #should be Manhattan
  if borough_finder.find_borough( 40.791061,-73.950348) != "Manhattan":
    print "Error - didn't find Manhattan on loop %d" % i
    exit()

  #should be Queens
  if borough_finder.find_borough( 40.713036,-73.874817) != "Queens":
    print "Error - didn't find Queens on loop %d" % i
    exit()
    
  #should be New Jersey
  if borough_finder.find_borough( 40.734181,-74.310425) != "New Jersey":
    print "Error - didn't find New Jersey on loop %d" % i
    exit()

duration = (time.time() - start_time)
print "--- %s seconds ---" % duration
