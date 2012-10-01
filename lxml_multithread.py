import lxml.html
from mysqloperation import DataSynch
import threading

class Test(threading.Thread):

    def __init__(self, threadname, thread_no=0):
        threading.Thread.__init__(self,name=threadname)
        self.datasyn = DataSynch(thread_no)

    def test(self):
        #seedurl_list = file('seedurls.file').readlines()
    	self.datasyn.cur.execute("select url from crawlerseedurl where deleted=0 and photosourcetype_id=2")
        seeds = self.datasyn.cur.fetchall()
        for url in seeds:
            if url is not None and len(url) > 0:
                print self.getName() + " - " + url[0]
                try:
                    tree = lxml.html.parse(url[0])
                    #tag  = tree.xpath('//a/@href')
                    #print tag
                except:
                    pass
    def run(self):
        while True:
            self.test()

    def stop(self):
        pass
tt = []
for i in range(5):
    tt.append(Test("Thread_no_" + str(i), i))
for i in range(5):
    tt[i].start()
for i in range(5):
    tt[i].join()
for i in range(5):
    tt[i].stop()
