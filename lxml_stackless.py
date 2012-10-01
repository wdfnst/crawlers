import urllib2
import time
import lxml.html
#import mysqloperation
import threading
import stackless
#import redis
import random

#redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0)
seedurl_list = file('seedurls.file').readlines()
#f = file('seedurls_1.file')
class Test:

    def __init__(self, i):
        self.num = i
        self.urllist = seedurl_list[i * 100:]

    def test(self, seedurl_list, i):
        #for url in self.urllist:
        for url in self.urllist:
            if url:
                print str(self.num) + ' - ' + url
                try:
                    r = urllib2.Request(url)
                    r.add_header('User-Agent', 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')
                    response = urllib2.urlopen(r)
                    content = response.read()
                    stackless.schedule()
                    tree    = lxml.html.fromstring(content)
                    #tree = lxml.html.parse(url)
                    tag  = tree.xpath('//a/@href')
                    print tag
                except:
                    pass

    def parse(self, i):
        counter = 0
        while True:
            ##url = redis_conn.rpop('urllist')
            ##url = seedurl_list[random.randint(0, 19332)]
            ##url = f.readline()
            #counter += 1
            #url = seedurl_list[i * 100 + counter]
            #if url:
                #print str(self.num) + ' - ' + url
                #try:
                    #r = urllib2.Request(url)
                    #r.add_header('User-Agent', 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')
                    #response = urllib2.urlopen(r)
                    #content = response.read()
                    #stackless.schedule()
                    #tree    = lxml.html.fromstring(content)
                    ##tree = lxml.html.parse(url)
                    #tag  = tree.xpath('//a/@href')
                    #print tag
                #except:
                    #pass
            self.test(seedurl_list, i)

tt = []
#for url in seedurl_list:
    #redis_conn.lpush('urllist', url)
for i in range(200):
    tt.append(Test(i))
    print i
    stackless.tasklet(tt[i].parse)(i)
stackless.run()
#tt = Test(1)
#stackless.tasklet(tt.test)(seedurl_list, 1)
#stackless.run()
#time.sleep(10)
