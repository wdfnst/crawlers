#!/bin/python
# -*- coding: utf-8 -*-
from urlparse import urlparse
import re

class Util(object):

	def find_domain(self, url):
		pos = url.find('/')
		if pos == -1:
		   pos = url.find('?')
		   if pos == -1:
			   return url
		url = url[0:pos]
		return url

	def get_domain(self, url):
		if url is None:
			return url
		# Process stardard urls
		if url.startswith('http://') or url.startswith('https://'):
			parts = urlparse(url)
			return parts.netloc
		# Process nostartd urls
		else:
			return self.find_domain(url)
	
	def standardize_url(self, base_url, raw_url):
		if raw_url == None or raw_url == "":
			return raw_url
		import urlparse
		url = urlparse.urljoin(base_url, raw_url)
		if url.startswith("http://www") or url.startswith("wwww") or url.startswith("https://www"):
			pass
		else:
			url = url.replace("//", "//www", 1)
		return url
	
	def concat_image_url(self, base_url, raw_url):
		if raw_url == None or raw_url == "" or raw_url.startswith("http://") or raw_url.startswith("https://"):
			return raw_url
		import urlparse
		url = urlparse.urljoin(base_url, raw_url)
		if url.startswith("http://www") or url.startswith("wwww") or url.startswith("https://www"):
			pass
		else:
			url = url.replace("//", "//www", 1)
		return url

	def getcompleteurl(self, incompleteurl):
		if incompleteurl.startswith("http://www") or incompleteurl.startswith("wwww") or incompleteurl.startswith("https://www"):
			return incompleteurl
		else:
			return incompleteurl.replace("//", "//www", 1)

	# Get urls from strings
	def get_url_from_strings(self, strs):
		substr_re = r"'(.*?)'"
		newurllist = []
		if type(strs) == tuple or type(strs) == list:
			for ss in strs:
				subs = re.findall(substr_re, ss)
				if len(subs) > 0:
					#strs.remove(ss)
					newurllist.append(subs[0])
				else:
					newurllist.append(ss)
		else:
			subs = re.findall(substr_re, strs)
			if len(subs) > 0:
				newurllist = subs
			else:
				newurllist.append(strs)
		return sorted(set(newurllist), key=newurllist.index)

	# Get the first no null element in a list
	def getfirstnullelement(self, ll):
		singlelist = []
		if len(ll) <= 0:
			return singlelist
		for l in ll:
			if l != None and l != "":
				singlelist.append(l)
				return singlelist
		return singlelist

	# convert embeded tuple or list to no-embed list
	def tuplelist2list(self, tl):
		newlist = []

		if type(tl) is not tuple and type(tl) is not list:
			newlist.append(tl)
			return newlist

		if len(tl) <= 0:
			return newlist
		for el in tl:
			if type(el) == tuple or type(el) == list:
				#el = re.sub(r'^\s', '', el)
				#el = re.sub(r'\s$', '', el)
				newlist.extend(el)
			else:
				newlist.append(el)
		# Remove duplicated element
		newlist = sorted(set(newlist), key=newlist.index)
		return filter(None, newlist)
	
	# Convert url or url-list to complete url
	def convt2compurl(self, base_url, urls):
		import re
		import urlparse
		if type(urls) is tuple or type(urls) is list:
			newurllist = []
			for url in urls:
				url = re.sub(r'^\s+', '', url)
				url = re.sub(r'\s+$', '', url)
				newurl = urlparse.urljoin(base_url, url)
				newurl = self.normalize_url(newurl)
				newurllist.append(newurl)
			return newurllist
		else:
			url = re.sub(r'^\s', '', url)
			url = re.sub(r'\s$', '', url)
			newurl = urlparse.urljoin(base_url, urls)
			return self.normalize_url(newurl)

	def get_start_one(self, la):
		if la is not None and type(la) is list and len(la) > 0:
			return la[0]
		else:
			return ""


	def normalize_url(self, curl):
		if not(curl):
			return curl

		# Converting the scheme and host to lower case
		# HTTP://www.Example.com/ → http://www.example.com/
		# Shopbop sense this
		# curl = curl.lower()

		# get rid of end's "/"
		if curl.endswith("/"):
			curl = curl[0:-1]

		return curl

		# Capitalizing letters in escape sequences. 
		# All letters within a percent-encoding triplet (e.g., "%3A") are case-insensitive, and should be capitalized. Example:
		# http://www.example.com/a%c2%b1b → http://www.example.com/a%C2%B1b 

		# Removing dot-segments. The segments “..” and “.” are usually removed from a URL according to 
		# the algorithm described in RFC 3986 (or a similar algorithm). Example:
		# http://www.example.com/../a/b/../c/./d.html → http://www.example.com/a/c/d.html 

		# Removing the default port. The default port (port 80 for the “http” scheme) may be removed from (or added to) a URL. Example:
		# http://www.example.com:80/bar.html → http://www.example.com/bar.html 

#uu = Util()
#print uu.get_domain("www.baidu.com")
#print uu.get_domain("www.baidu.com.cn/asdf/asdf")
#print uu.get_domain("ishare.iask.sina.com.cn")
#print uu.get_domain("www1.baidu.com.cn")
#print uu.get_domain("wwww.baasdfds.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("http://wwww.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("https://wwww.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("https://wwww1.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("https://wwwwa.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print "-----------------"
#print uu.standardize_url("http://www.baidu.com", "a.html")
#print uu.standardize_url("http://www.baidu.com/b.html", "c.html")
#print uu.standardize_url("http://www.baidu.com/a/b/c.html", "d.html")
#print uu.standardize_url("http://www.baidu.com?a=1&b=2", "a.html")
#print uu.standardize_url("http://www.baidu.com/a.html?a=1&b=2", "d.html")
