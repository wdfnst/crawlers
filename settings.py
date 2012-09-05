import re

class Settings(object):

	pattern = re.compile(r'.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])[-/]$|.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])$|.*(\d{4})[-/](0?[1-9]|1[012])$|.*(\d{4})[-/](0?[1-9]|1[012])[-/]$|.*(\?showComment).*|.*(/page/).*|.*(\?=p).*|.*(\?cat=).*|.*(\?=page).*|.*(_archive).*|.*(#).*|.*(/search\?).*|.*(search/label).*|.*(index\.).*|.*(\?paged=).*|.*(/category/).*|.*(january)$|.*(january)/$|.*(february)$|.*(february)/$|.*(march)$|.*(march)/$|.*(april)$|.*(april)/$|.*(may)$|.*(may)/$|.*(june)$|.*(june)/$|.*(july)$|.*(july)/$|.*(august)$|.*(august)/$|.*(september)$|.*(september)/$|.*(october)$|.*(october)/$|.*(november)$|.*(november)/$|.*(december)$|.*(december)/$')

	mysqlsettings   = {'host':'localhost', 'port':3306, 'user':'fashion', 'passwd':'lmsi3229fashion', 'db':'fashion4_ads_test_1', 'charset':'utf8'}

	redissettings   = {'host':'localhost', 'port':6379, 'db':0}

	crawlername     = ['BlogCrawler', 'ProductCrawler']
	"""
	crawlersettings = [{'page_url_set':'blog_page_url_set', 'nothrow_urljson_list':'blog_nothrow_urljson_list', 'image_url_set':'blog_image_url_set', \
								'process_no':25, 'thread_no':12},\
					   {'page_url_set':'product_page_url_set', 'nothrow_urljson_list':'product_nothrow_urljson_list', 'image_url_set':'product_image_url_set', \
								'process_no':25, 'thread_no':12},\
					   {'page_url_set':'form_page_url_set', 'nothrow_urljson_list':'form_nothrow_urljson_list', 'image_url_set':'form_image_url_set', \
								'process_no':25, 'thread_no':12}\
					  ]
	setlistsettings = {'page_url_set':'page_url_set', 'nothrow_urljson_list':'nothrow_urljson_list', 'image_url_set':'image_url_set'}
	"""

	"""
	Init_level: 1 read in seeds and del *_page_url_set
				2 del *_page_url_set, nothrow_urljson_list
				3 del *_page_url_set,  *_image_url_set, nothrow_urljson_list
				4 del all: *_page_url_set, *_image_url_set, *_seeds_set, nothrow_urljson_list
				5 read the seeds from redis' *_seeds_set
				6 do nothing, and go directly
	"""
	blogcrawlersettings    = {'page_url_set':'blog_page_url_set', 'nothrow_urljson_list':'blog_nothrow_urljson_list', 'image_url_set':'blog_image_url_set', \
								'seeds_set':'blog_seeds_set', 'process_no':20, 'thread_no':12, 'init_level':1}
	productcrawlersettings = {'page_url_set':'product_page_url_set', 'nothrow_urljson_list':'product_nothrow_urljson_list', 'image_url_set':'product_image_url_set', \
								'seeds_set':'product_seeds_set', 'process_no':10, 'thread_no':12, 'init_level':1}
	formcrawlersettings    = {'page_url_set':'form_page_url_set', 'nothrow_urljson_list':'form_nothrow_urljson_list', 'image_url_set':'form_image_url_set', \
								'seeds_set':'form_seeds_set', 'process_no':25, 'thread_no':12, 'init_level':1}
	
	crawlersettings        = [blogcrawlersettings, productcrawlersettings]

	foldersettings		   = {'original_image_folder':'/public2/ads/image_test/original_image_folder/'}

	crawlertype = {'crawlertype':2}
	depth_limit = 1000

	seeds = {}
