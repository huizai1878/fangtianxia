# -*- coding: utf-8 -*-
import scrapy
import re
from fangtianxia.items import NewHouseItem, ESFHouseItem

class FangSpider(scrapy.Spider):
    name = 'fang'
    allowed_domains = ['fang.com']
    start_urls = ['https://www.fang.com/SoufunFamily.htm']

    def parse(self, response):
        trs = response.xpath("//div[@class='outCont']//tr")
        province = None
        for tr in trs:
            tds = tr.xpath(".//td[not(@class)]")
            province_td = tds[0]
            # 获取省份
            province_text = province_td.xpath(".//text()").get()
            province_text = re.sub(r'\s', '', province_text)
            if province_text:
                province = province_text
            # 排除国外
            if province == '其它':
                continue
            city_td = tds[1]
            city_links = city_td.xpath(".//a")
            for city_link in city_links:
                city = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()
                # 构建新房的url链接
                url_module = city_url.split(".")
                newhouse_url = url_module[0] + '.newhouse.' + url_module[1] + '.' + url_module[2] + 'house/s/'
                # 构建二手房的url链接
                esf_url = url_module[0] + '.esf.' + url_module[1] + '.' + url_module[2]
                # 请求新房链接
                yield scrapy.Request(url=newhouse_url, callback=self.parse_newhouse, meta={'info': (province, city)})
                # 请求二手房链接
                yield scrapy.Request(url=esf_url, callback=self.parse_esf, meta={'info': (province, city)})

    def parse_newhouse(self, response):
        province, city = response.meta.get('info')
        lis = response.xpath("//div[@id='newhouse_loupai_list']/ul/li")
        for li in lis:
            name = li.xpath(".//div[@class='nlcd_name']/a/text()").get()
            if name == None:
                continue
            name = re.sub('\s', '', str(name))
            price = "".join(li.xpath(".//div[@class='nhouse_price']//text()").getall())
            price = re.sub(r'\s|广告', '', price)
            house_type_list = li.xpath(".//div[contains(@class, 'house_type')]/a/text()").getall()
            house_type_list = list(map(lambda x:re.sub(r'\s', '', x), house_type_list))
            rooms = '/'.join(list(filter(lambda x:x.endswith('居'), house_type_list)))
            area_list = li.xpath(".//div[contains(@class, 'house_type')]//text()").getall()
            if area_list:
                area = re.sub(r'\t|\n|－', '', area_list[-1])
            else:
                area = None
            address = li.xpath(".//div[@class='address']/a/@title").get()
            sale = li.xpath(".//div[contains(@class, 'fangyuan')]/span/text()").get()
            origin_url = li.xpath(".//div[@class='nlcd_name']/a/@href").get()

            item = NewHouseItem(name=name, rooms=rooms, area=area, address=address, sale=sale,
                                price=price, origin_url=response.urljoin(origin_url), province=province, city=city)
            yield item

        next_url = response.xpath("//div[@class='page']//a[@class='next']/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_newhouse,
                                 meta={'info': (province, city)})


    def parse_esf(self, response):
        province, city = response.meta.get('info')
        dls = response.xpath("//div[contains(@class, 'shop_list')]/dl")
        for dl in dls:
            item = ESFHouseItem(province=province, city=city)
            item['name'] = dl.xpath(".//p[@class='add_shop']/a/@title").get()
            if not item['name']:
                continue
            infos = dl.xpath(".//p[@class='tel_shop']/text()").getall()
            infos = list(map(lambda x:re.sub(r'\s', '', x), infos))
            for info in infos:
                if '厅' in info:
                    item['rooms'] = info
                elif '㎡' in info:
                    item['area'] = info
                elif '层' in info:
                    item['floor'] = info
                elif '向' in info:
                    item['toward'] = info
                elif '建' in info:
                    item['year'] = info
                else:
                    continue
            item['address'] = dl.xpath(".//p[@class='add_shop']/span/text()").get()
            item['price'] = ''.join(dl.xpath(".//dd[@class='price_right']/span[@class='red']//text()").getall())
            item['unit'] = dl.xpath(".//dd[@class='price_right']/span[2]/text()").get()
            detail_url = dl.xpath(".//h4[@class='clearfix']/a/@href").get()
            item['origin_url'] = response.urljoin(detail_url)
            yield item
        next_url = response.xpath(".//div[@class='page_al']/p[1]/a/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_esf,
                             meta={'info': (province, city)})





