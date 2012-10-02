
## Python-Crawler

The Python Crawler: BlogCrawler and ProductCrawler

## 1. Requirements:

### 1.)Python Packages:

python-redis: Python persistent object library based on Redis.  
DBUtils: The Commons DbUtils library is a small set of classes designed to 	make working with JDBC easier, and provide the ability to use the connection 	pool in python.
MySQLdb: MySQLdb is a Python DB API-2.0-compliant interface.
lxml: the most feature-rich and easy-to-use library for processing XML and 	HTML in the Python language. 
Psutil: Psutil is a module providing an interface for retrieving information on 	all running processes and system utilization (CPU, memory, disks, network, 	users) in a portable way by using Python.
Samplejson, urllib2, PIL.
### 2.)Redis install: 
Usually, 8GB memory should preserve to redis-server.
Note: Intalling guide attached in document [1]
### 3.)MySQL:
 A default max_connections value should be setted as 300(25 * 36) at least, but 1000 is recommended, also it's a optimal mount to live with other mysql-connection consumption processes.

## 2. Configures:
### 1.)Redis configure: 
Detailed statement shown in attached document [1].
Note: Usually don't try to change any parameter once it's configured well. If you want to migrate the redis-server and data, you could simply copy the configure file and data files which specified in the document [1].
### 2.)Mysql configure:
 Using "set GLOBAL max_connections=1000" to set the mysql's max_connections option.
Note: Usually don't try to change any parameter once it's configured well.
### 3.)BlogCrawler configure:
The following items in ***settings.py represents:
a.) mysqlsettings: host, port, user, passwd, db and charset are required.
  Note: utf8 shall be a good choice for charset
b.) redissettings: host, port and db are required
  Note: setting db to be a separated number between crawlers is required, and a nonzero between 1 to 16 is recommended for db 0 is a default db that may lead to incorrect operation.
c.) blogcrawlersettings:
  page_url_set: A set that cache all the webpage urls' hash(url).
  nothrow_urljson_list: A queue cache the json of 
    {'url': 'http://', 'pagetype': 2L, 'depth': 5, 'seed_id': 632L}
  image_url_set: A set cache all the image urls' hash(url).
  process_no: Define the number of the process to start initially.
  thread_no: Define the number of the threads in each process.
  init_level: Define the crawler's initialization level when starts, 0 means do 
  nothing, 1 means deleting all the list and preserve all the set, 2 means 
  deleting all the list and page_url_set , all the list and page_url_set, 3   
  means deleting all the lists and sets. 
d.) foldersettings: Setting the folder to store the images, before setting the folder, you should confirm this folder containing all the sub-folders which sequentially generated from 000 to fff(a tri-Alphanumeric sequence)
e.) crawlertype: This parameter is same with photosourcetype_id in mysql's table crawlerseedurl, it means the photos' source, 2 presents coming from blog, 3 presents coming from product websites.
f.) depth_limit: To limit the crawler's depth.
Note: Usually don't try to change any parameter once it's configured well.

### 4.)Additional configures for product crawler:
For product crawler is desiged to crawl designated elements in pages, so in all the product seeds which marked by photosourcetype_id=3 in mysql table crawlerseedurl should contain the xpath corresponding to target element. The following sql should be executed after getting the xpath.
update crawlerseedurl set photosourcetype_id=3 where url like "%your url%";
update crawlerseedurl  set detailurl_xpath="your xpath", nextpage_xpath="your xpath" where url like "%your url%";
update crawlerseedurl set TITLE_XPATH="", IMAGEURL_XPATH="", BRAND_XPATH="", DESCRIPTION_XPATH="", PRODUCTID_XPATH="", COLOR_XPATH="", PRICE_XPATH="", SIZE_XPATH="", MAINIMAGEURL_XPATH="" where url like "%your url%";
Note: How to configure your robust xpath, you may need to reference document 		[2]
## 3. How to run the instance of the crawler
In the workstation(137.132.145.238), you could type the following command to start a crawler instance after configuring:

>> nohup python2 BlogCrawler.py > output.file &

Note: python2 means python2.6 for there are multiple python versioins installed in workstation. So if on your own machine, you just need to start the crawler intances by typing: python BlogCrawler.py after intalling and configuring. 
## 4. How to inspect the status of redis
### 1.)The most promptly way is using redis-cli to run the monitor command "redis-cli monitor", then all the command that running in redis-server could be observed immediately. But it's a resource consumption way. Usually you could get into the "redis-cli", and then use "llen blog_nothrow_urljson_list", "scard blog_page_url_set" and other similar commands to keep watch over the status of the redis-server and crawlers.
### 2.)The crawler's log file also provide a very straightforward way to moniter the crawlers.
### 3.)The Redis' log file is another way to inspect the status of the redis-server, you could get the connection number and the size of the data cached in redis, , you could use command "sudo tail -f /var/log/redis_6379.log"(change  the name according to your configure file name) to view the redis' log.

## 5. The component diagram
Shown in attached document [3]
## 6. The class diagram
Shown in attached document [4]
## 7. The process flow diagram
Shown in attached document [5]


## Appendix :
[1] Redis installing and configuring guide
https://docs.google.com/file/d/0B79kk8CpytwrRmVILXZxODdBcGM/edit§ 
[2] How to configure your robust xpath
http://www.w3schools.com/xpath/xpath_syntax.asp§
[3] 
[4] 
[5] 
