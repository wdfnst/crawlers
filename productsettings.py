#!/bin/python
# -*- coding: utf-8 -*-
import re

class Settings(object):

	mysqlsettings   = {'host':'137.132.145.238', 'port':3306, 'user':'visenze', 'passwd':'lmsi3229visenzefashion', 'db':'visenze_fashion', 'charset':'utf8'}

	redissettings   = {'host':'localhost', 'port':6379, 'db':3}

	productcrawlersettings = {'page_url_set':'product_page_url_set', 'nothrow_urljson_list':'product_nothrow_urljson_list', 'image_url_set':'product_image_url_set', \
								'seeds_set':'product_seeds_set', 'process_no':1, 'thread_no':1, 'init_level':0}

	foldersettings		   = {'original_image_folder':'/public2/ads/image_test/original_image_folder/'}

	crawlertype = {'crawlertype':3}

	depth_limit = 1000
