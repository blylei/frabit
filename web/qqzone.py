from selenium import webdriver
import time
import requests
# 导入密钥构造类
from get_g_tk import GetGTK
from lxml import etree
import demjson
import pymongo

myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['QQDongTaiInfo']
mycollection = mydb['QQDongTaiInfo']


class GetQQDongTaiInfo(object):
    chrome_driver = r'E:\迅雷下载\chromedriver_win32\chromedriver.exe'

    def __init__(self, username, password):
        self.driver = webdriver.Chrome(executable_path=GetQQDongTaiInfo.chrome_driver)
        self.cookies = {}
        self.username = username
        self.password = password
        self.base_url = 'https://user.qzone.qq.com/proxy/domain/ic2.qzone.qq.com/cgi-bin/feeds/feeds3_html_more?uin={}&begintime={}&g_tk={}'
        # g_tk为jquery中加密的字段，用登陆的cookie信息进行加密
        self.g_tk = None
        self.begintime = None

    def login_qq_zone(self):
        self.driver.get('https://qzone.qq.com/')

        # 切换网页框架
        self.driver.switch_to.frame(self.driver.find_element_by_id('login_frame'))

        # 切换到账户密码输入界面
        self.driver.find_element_by_id('switcher_plogin').click()

        # 输入账号
        self.driver.find_element_by_id('u').clear()
        self.driver.find_element_by_id('u').send_keys(self.username)

        # 输入密码
        self.driver.find_element_by_id('p').clear()
        self.driver.find_element_by_id('p').send_keys(self.password)

        # 登陆账号
        self.driver.find_element_by_id('login_button').click()
        time.sleep(3)
        self.driver.find_element_by_xpath('//*[@id="tab_menu_friend"]/div[3]').click()
        time.sleep(3)
        self.cookies = {i['name']: i['value'] for i in self.driver.get_cookies()}

    def get_static_html_info(self):
        page_source = self.driver.page_source
        self.begintime = self.driver.find_elements_by_xpath(
            '//*[@id="feed_friend_list"]//li[@class="f-single f-s-s"]').pop().get_attribute(
            'id').split('_')[4]
        html = etree.HTML(page_source)
        # 获取静态网页中的动态消息
        dongtai_contents = html.xpath('//li[@class="f-single f-s-s"]')
        # print(dongtai_content)
        single_info = dict()
        for temp in dongtai_contents:
            # 动态内容
            single_info['content'] = temp.xpath(".//div[starts-with(@id,'feed_')]/div[@class='f-info']/text()")
            # print(single_info['content'])
            # 动态发布者名称
            single_info['publisher_name'] = temp.xpath(".//a[contains(@class,'f-name')]/text()")
            # 动态发布时间戳
            single_info['push_date'] = temp.xpath(".//*[starts-with(@id,'hex_')]/i/@data-abstime")
            # 动态浏览次数
            single_info['view_count'] = temp.xpath(".//a[contains(@class,'state qz_feed_plugin')]/text()")
            # 动态评论
            single_info['comments-content'] = temp.xpath(".//div[@class='comments-content']//text()")
            # 点赞次数
            # print(temp.xpath(".//span[@class='f-like-cnt']/text()"))
            single_info['like'] = temp.xpath(".//span[@class='f-like-cnt']/text()")
            # print(single_info)
            self.save_to_mongdb(single_info)
        self.cookies = {i['name']: i['value'] for i in self.driver.get_cookies()}
        cookie_str = ''
        for key, value in self.cookies.items():
            cookie_str += key + '=' + value + '; '
        self.g_tk = GetGTK(cookie_str).run()

    def get_dynamic_info(self):

        requests_url = self.base_url.format(self.username, self.begintime, self.g_tk)
        print(requests_url)
        res = requests.get(requests_url, cookies=self.cookies).content.decode()
        res_dict = demjson.decode(res[10: -3])
        # 如果没有请求到正确数据，再次发出请求
        try:
            res_datas = res_dict['data']['data']
        except KeyError:
            self.get_dynamic_info()
            return None

        res_datas = [temp for temp in res_datas if isinstance(temp, dict)]
        # res_datas_len = len(res_datas)
        for temp in res_datas:
            single_info = dict()
            html = etree.HTML(temp['html'])
            # 动态内容
            single_info['content'] = html.xpath("//div[@class='f-info']/text()")
            # print(single_info['content'])
            # 动态发布者名称
            single_info['publisher_name'] = temp['nickname']
            # 动态发布时间戳
            single_info['push_date'] = temp['abstime']
            # 动态浏览次数
            single_info['view_count'] = html.xpath("//a[@class='state qz_feed_plugin']/text()")
            # 动态评论
            single_info['comments-content'] = html.xpath("//div[@class='comments-content']//text()")
            # 点赞次数
            # print(temp.xpath(".//span[@class='f-like-cnt']/text()"))
            single_info['like'] = html.xpath(".//span[@class='f-like-cnt']/text()")
            # print(single_info)
            self.save_to_mongdb(single_info)
            if temp == res_datas[-1]:
                self.begintime = single_info['push_date']
                self.get_dynamic_info()

    def save_to_mongdb(self, single_info):
        if mycollection.find({'push_date': single_info['push_date']}).count() == 0:
            mycollection.insert_one(single_info.copy())
            print('插入成功')
        else:
            print('插入失败')

    def run(self):
        self.login_qq_zone()
        self.get_static_html_info()
        self.get_dynamic_info()


if __name__ == "__main__":
    username = '***'  # qq账号
    password = '***'  # qq密码
    Demo = GetQQDongTaiInfo(username, password)
    Demo.run()

