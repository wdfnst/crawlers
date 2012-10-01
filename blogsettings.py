import re

class Settings(object):

	pattern = re.compile(r'.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])[-/]$|.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])$|.*(\d{4})[-/](0?[1-9]|1[012])$|.*(\d{4})[-/](0?[1-9]|1[012])[-/]$|.*(\?showComment).*|.*(/page/).*|.*(\?=p).*|.*(\?cat=).*|.*(\?=page).*|.*(_archive).*|.*(#).*|.*(/search\?).*|.*(search/label).*|.*(index\.).*|.*(\?paged=).*|.*(/category/).*|.*(january)$|.*(january)/$|.*(february)$|.*(february)/$|.*(march)$|.*(march)/$|.*(april)$|.*(april)/$|.*(may)$|.*(may)/$|.*(june)$|.*(june)/$|.*(july)$|.*(july)/$|.*(august)$|.*(august)/$|.*(september)$|.*(september)/$|.*(october)$|.*(october)/$|.*(november)$|.*(november)/$|.*(december)$|.*(december)/$')

	mysqlsettings   = {'host':'weardex.com', 'port':3306, 'user':'fashion', 'passwd':'lmsi3229fashion', 'db':'fashion4_ads_test_1', 'charset':'utf8'}

	redissettings   = {'host':'localhost', 'port':6379, 'db':1}

	blogcrawlersettings    = {'page_url_set':'blog_page_url_set', 'nothrow_urljson_list':'blog_nothrow_urljson_list', 'image_url_set':'blog_image_url_set', \
								'seeds_set':'blog_seeds_set', 'process_no':20, 'thread_no':36, 'init_level':0}
	
	foldersettings		   = {'original_image_folder':'/public2/ads/image_test/original_image_folder/'}

	crawlertype = {'crawlertype':2}

	depth_limit = 1000
