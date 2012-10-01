import lxml.html
import mysqloperation


#self.datasyn = DataSynch(thread_no)
def main():
        
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


main()
