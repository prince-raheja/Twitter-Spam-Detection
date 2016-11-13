# This code assumes that every tweet contains only one url
import math
from pyspark import SparkConf, SparkContext

# setting up local server for running spark
conf = (SparkConf().setMaster("local").setAppName("My app").set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

twitter_data_file = raw_input('Enter file location : ')
twitter_file = sc.textFile(twitter_data_file)

# rdd containing tuples of form (user, url)
users_urls = twitter_file.map(lambda x : tuple(x.split()))

# rdd containing all urls posted by all users
urls = twitter_file.map(lambda x : x.split()[1].strip().rstrip())
total_urls_count = urls.count()

# rdd containing urls with their count of form (url, count)
url_count = urls.map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y)


# finding the probability of a url
url_share = url_count.mapValues(lambda x : math.log10(total_urls_count/float(x)) )
# url_share.persist()

# using broadcast to access url_share values inside of a map function
url_share_broadcast = sc.broadcast(url_share.collectAsMap())

# users_url_count = users_urls.map(lambda x : (x[0], (x[1], 1))).reduceByValue
# groups all urls of individual users in form  (user, list_of_all_urls)
user_urls_grouped = users_urls.groupByKey().map(lambda x : (x[0], list(x[1])))

def user_url_frequency(entry):
	# wrong code as sparkcontext can only be invoked on driver node i.e. sparkcontext cannot be invoked inside a custom function
 	# url_list = sc.parallelize(entry[1]).map(lambda x : (x,1)).reduceByValue(lambda x,y : x+y)
 	# return (entry[0], url_list)
 	url_count = {}
 	# print entry
 	for url in entry:
 		try:
 			url_count[url] += 1
 		except KeyError:
 			url_count[url] = 1
 	print url_count
 	# return (entry[0], url_count)
 	return url_count



# stores info of every user of form (user, dictionay with key = url, value = count)
user_urls_count = user_urls_grouped.mapValues(user_url_frequency)
user_urls_count = user_urls_count.sortByKey()
user_urls_count.persist()


# find cartesian of users and filter 
users_cartesian = user_urls_count.cartesian(user_urls_count)
# remove the entries where both elements are same
users_cartesian = users_cartesian.filter(lambda x : x[0][0] != x[1][0])



def similarity_function(input):
	user1, user2 = input[0], input[1]
	print input[0]
	print input[1]
	user1_urls = user1[1]
	user2_urls = user2[1]
	urls_count_dict = {}
	for url, count in user1_urls.iteritems():
		try:
			urls_count_dict[url] = count + user2_urls[url]
		except KeyError:
			pass

	similarity = 0
	for url, value in urls_count_dict.iteritems():
		share = url_share_broadcast.value[url]
		similarity += value * share

	return (user1[0], (user2[0], similarity) )


sample_list = []
def temp_func(input_given):
	global sample_list
	sample_list.append(5)
	print sample_list


# find similarity between the users using formula (n1 + n2)*share(url)   n1 - occurence of url in user1 and n2 - occurence of url in user2
users_cartesian = users_cartesian.map(similarity_function)
users_cartesian_grouped = users_cartesian.groupByKey().mapValues(lambda x : list(x))


# work to do 
# see how to iterate over rdd's to find cohesive graphs


# def insert_in_set(node_name):
# 	global length_set , nodes_traveled
# 	nodes_traveled.add(node_name)
# 	length_set += 1

# def insert_in_list(node):
# 	global nodes_ordered, length_list
# 	nodes_ordered.append(node)
# 	length_list += 1

length_set = 0
nodes_traveled = set()
nodes_ordered = []
first_node = users_cartesian_grouped.first()
nodes_traveled.add(first_node[0])
length_set += 1

# current_node_name is string and current_node_list is the list of all (node_name, similarity) pairs 
# corresponding to current_node_name 
current_node_name = first_node[0]
current_node_list = first_node[1]


# fills nodes_ordered list by values (node_1, node_2, similarity), (node_2, node_3, similarity) tuples 
# for finding out cohesive graphs
while length_set != users_cartesian_grouped.count():
	maximum = -1
	max_node = None
	for node in current_node_list:
		if node[0] not in nodes_traveled and node[1] > maximum :
			maximum = node[1]
			max_node = node

	
	# max_node is of from (node_name, similarity)
	nodes_traveled.add(max_node[0])
	length_set += 1
	nodes_ordered.append((current_node_name, max_node[0], max_node[1]))
	current_node_name = max_node[0]
	# returns a list of all elements of key max_node[0]  list have only one value 
	current_node_list = users_cartesian_grouped.lookup(max_node[0])[0] 