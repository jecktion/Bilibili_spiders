# -*- coding: utf-8 -*-
# 此程序用来抓取爱奇艺的数据
import hashlib
import os

import requests
import time
import random
import re
from multiprocessing.dummy import Pool
import csv
import json
import sys
from fake_useragent import UserAgent, FakeUserAgentError
from save_data import database

class Spider(object):
	def __init__(self):
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		# self.date = '2000-10-01'
		# self.limit = 500000
		self.db = database()
	
	def get_headers(self):
		# user_agent = self.ua.chrome
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
					   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
					   'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
					   'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
					   'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
					   'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
					   'Opera/9.52 (Windows NT 5.0; U; en)',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
					   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
					   'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'host': "api-t.iqiyi.com",
		           'connection': "keep-alive",
		           'user-agent': user_agent,
		           'accept': "*/*",
		           'referer': "http://www.iqiyi.com/lib/m_216426014.html?src=search",
		           'accept-encoding': "gzip, deflate",
		           'accept-language': "zh-CN,zh;q=0.9"
		           }
		return headers
	
	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub('/', ";", x)
		x = re.sub(re.compile('\s{2,}'), ' ', x)
		x = re.sub('[\n\r]', ' ', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HI18001I69T86X6D"
		proxyPass = "D74721661025B57D"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies
	
	def get_comments_page(self, s):
		film_url, product_number, plat_number, oid, page = s
		print page
		url = "https://api.bilibili.com/x/v2/reply"
		querystring = {"pn": str(page), "type": "1", "oid": oid, "sort": "0"}
		headers = {
			'host': "api.bilibili.com",
			'connection': "keep-alive",
			'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
			'accept': "*/*",
			'referer': "https://www.bilibili.com/video/av12631457/",
			'accept-encoding': "gzip, deflate, br",
			'accept-language': "zh-CN,zh;q=0.9"
		}
		
		retry = 5
		while 1:
			try:
				text = \
					requests.get(url, headers=headers, proxies=self.GetProxies(), timeout=10,
					             params=querystring).json()['data']['replies']
				last_modify_date = self.p_time(time.time())
				results = []
				for item in text:
					nick_name = item['member']['uname']
					cmt_time = self.p_time(item['ctime'])
					cmt_date = cmt_time.split()[0]
					# if cmt_date < self.date:
					# 	continue
					comments = self.replace(item['content']['message'])
					like_cnt = str(item['like'])
					cmt_reply_cnt = str(item['rcount'])
					long_comment = '0'
					src_url = film_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, src_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				if len(results) > 0:
					return results
				else:
					return None
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_total_page(self, oid):  # 获取评论总页数
		url = "https://api.bilibili.com/x/v2/reply"
		querystring = {"pn": "1", "type": "1", "oid": oid, "sort": "0"}
		headers = {
			'host': "api.bilibili.com",
			'connection': "keep-alive",
			'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
			'accept': "*/*",
			'referer': "https://www.bilibili.com/video/av12631457/",
			'accept-encoding': "gzip, deflate, br",
			'accept-language': "zh-CN,zh;q=0.9"
		}
		retry = 5
		while 1:
			try:
				text = \
					requests.get(url, headers=headers, proxies=self.GetProxies(), timeout=10,
					             params=querystring).json()['data']['page']['count']
				total = int(text)
				# if total > self.limit:
				# 	total = self.limit
				if total % 20 == 0:
					pagenums = total / 20
				else:
					pagenums = total / 20 + 1
				return pagenums
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_movie_id(self, film_url):  # 获取电影id
		if u'/av' in film_url:
			p = re.compile('\d{3,}')
			oid = re.findall(p, film_url)[0]
			return [oid]
		retry = 5
		while 1:
			try:
				text = requests.get(film_url, proxies=self.GetProxies(), timeout=10).text
				# print text
				p0 = re.compile(u'\{"aid":(\d+?),')
				oid = re.findall(p0, text)
				return oid
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_comments_all(self, film_url, product_number, plat_number):  # 获取所有评论
		videoId = self.get_movie_id(film_url)
		if videoId is None:
			print u'%s 评论抓取出错' % product_number
			return None
		else:
			print u'%s 共有 %d 集' % (product_number,len(videoId))
			oids = videoId
			for oid in oids:
				pagenums = self.get_total_page(oid)
				if pagenums is None:
					print u'%s 评论抓取出错' % product_number
					return None
				else:
					print u'%s 共有 %d 页' % (oid, pagenums)
					s = []
					for page in range(1, pagenums + 1):
						s.append([film_url, product_number, plat_number, oid, page])
					pool = Pool(5)
					items = pool.map(self.get_comments_page, s)
					pool.close()
					pool.join()
					mm = []
					for item in items:
						if item is not None:
							mm.extend(item)
					'''
					with open('data_comments.csv', 'a') as f:
						writer = csv.writer(f, lineterminator='\n')
						writer.writerows(mm)
					'''
					print u'%s 开始录入数据库' % product_number
					self.save_sql('T_COMMENTS_PUB_MOVIE', mm)  # 手动修改需要录入的库的名称
					print u'%s 录入数据库完毕' % product_number
	
	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in results:
			try:
				self.db.add(table_name, item)
			except:
				continue



if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s.append([i[2], i[0], 'P08'])
	for j in s:
		print j[1]
		spider.get_comments_all(j[0], j[1], j[2])
	spider.db.db.close()
