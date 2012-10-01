import lxml.html
import mysqloperation


#self.datasyn = DataSynch(thread_no)
class Test(threading.Thread):

    def __init__(self):
		threading.Thread.__init__(self,name=threadname)

    def test(self):
        seedurl_list = file('seedurls.file').readlines()
        for url in seedurl_list:
            if url is not None and url != "":
                print "- " + url
                try:
                    tree = lxml.html.parse(url)
                    #tag  = tree.xpath('//a/@href')
                    #print tag
                except:
                    pass
    def run(self):
        while True:
            self.test()

    def stop(self):
        pass

tt = Test()
tt.start()
tt.join()
tt.stop()
