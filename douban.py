# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 15:58:44 2018

@author: pengzetao

@内容：下载豆瓣top250的所有电影，并将其写入数据库之中。将数据存放在数据库'd:/data.db/douban'中

@具体的实现思路：下载页面（requests）--->解析页面中的内容---->将数据写入数据库中(sqlite3)

@遇到的问题：
1.豆瓣编辑内容的问题：在某个电影中主演的姓名是未知的，解决的方法就是把主演的名字用未知代换。
"""
import requests
import sqlite3
import re
from lxml import etree
from multiprocessing import Pool


class DownLoad(object):

    def __init__(self):
        """
        参数的初始化
        """
        # 头部数据的初始化
        self.headers = {}
        self.start_url = "https://movie.douban.com/top250"
        self.UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"
        self.headers['User-Agent'] = self.UA

        # 数据库的初始化
        print('开始连接数据库.....')
        self.connect = sqlite3.connect('d:\data.db')
        self.cursor = self.connect.cursor()
        print('数据库连接成功,开始创建表格....')
        self.cursor.execute('drop table if exists douban')
        self.cursor.execute("""create table douban(
        id integer primary key autoincrement,
        movie_name text,
        director text,
        actors text,
        year text,
        country text,
        movie_type text,
        score text,
        lines text)""")
        print('表格创建成功，等待爬虫程序的开始...')

        # 正则信息的初始化
        self.patt2 = ('主演: (\w*(·\w*)?)')
        self.patt1 = ('导演: (\w*(·\w*)?)')

    def split_information(self, informations):

        def find_index(data):
            index_list = []
            for i in range(len(data)):
                if data[i] == '/':
                    index_list.append(i)
            return index_list

        def catch_infor(one_list):
            result = ""
            for i in one_list:
                if i != one_list[-1]:
                    result = result + i + '/'
                else:
                    result += i
            return result
        data = informations.split()
        index_list = find_index(data)
        year = str(data[0])
        country = catch_infor(data[index_list[0] + 1:index_list[1]])
        movie_type = catch_infor(data[index_list[1] + 1:])
        return year, country, movie_type

    def get_page(self, url):
        """
        下载页面，返回网页对象
        """
        r = requests.get(url, headers=self.headers, timeout=3)
        r.encoding = r.apparent_encoding
        return r

    def get_data(self, page):
        """
        对下载的网页对象进行解析，获得想要的数据集和。
        """
        tree = etree.HTML(page.text)
        movies = tree.xpath(
            '//*[@id="content"]/div/div[1]/ol/li/div/div[2]/div[1]/a/span[1]')  # 电影名字的集合
        # 获得演员和导演的名字信息的集合
        names = tree.xpath(
            '//*[@id="content"]/div/div[1]/ol/li/div/div[2]/div[2]/p[1]/text()[1]')
        informations = tree.xpath(
            '//*[@id="content"]/div/div[1]/ol/li/div/div[2]/div[2]/p[1]/text()[2]')  # 获得上映时间、国家、电影类型的信息集合
        scores = tree.xpath(
            '//*[@id="content"]/div/div[1]/ol/li/div/div[2]/div[2]/div/span[2]')  # 获得电影的评分
        lines = tree.xpath(
            '//*[@id="content"]/div/div[1]/ol/li/div/div[2]/div[2]/p[2]/span')  # 获得电影的经典台词
        return movies, names, informations, scores, lines

    def store_data(
            self,
            movies,
            names,
            informations,
            scores,
            lines,
            page_index):
        """
        将数据存储到sqlite3数据库中。
        """
        length = len(movies)
        for i in range(length):
            year, country, movie_type = self.split_information(informations[i])
            movie_name = movies[i].text
            score = scores[i].text
            dialogue = lines[i].text
            try:
                actor = re.search(self.patt2, names[i].strip()).group(1)
            except BaseException:
                # print('获取主演的名字出现问题')
                actor = "未知"
            director = re.search(self.patt1, names[i].strip()).group(1)
            self.cursor.execute("""insert into douban (
                        movie_name, director, actors, year,
                        country, movie_type, score, lines)
                        values (?,?,?,?,?,?,?,?)
                        """, (movie_name, director, actor, year,
                              country, movie_type, score, dialogue))
            self.connect.commit()

    def crawl(self):
        """
        爬虫的控制节点
        """
        index = [x for x in range(0, 250, 25)]
        total = len(index)
        print('开始豆瓣爬虫工程.....')
        start_page = 1
        for x in index:
            print('进入第{}个页面的爬取，总共有{}个页面'.format(start_page, total))
            url = self.start_url + '?start=' + str(x)
            page = self.get_page(url)
            movies, names, informations, scores, lines = self.get_data(page)
            self.store_data(movies, names, informations, scores, lines, x)
            start_page += 1
        print('正在准备关闭数据库')
        self.cursor.close()
        self.connect.close()
        print('数据库关闭完毕，爬虫程序正式结束...')


if __name__ == "__main__":
    spider = DownLoad()
    spider.crawl()

