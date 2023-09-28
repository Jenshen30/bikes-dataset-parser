import multiprocessing

import cProfile

from bs4 import BeautifulSoup
import re

import csv
import random
import requests
from multiprocessing.pool import Pool


BASEURL = "https://www.velosklad.ru"


def slash_join(*args):
    return "/".join(arg.strip("/") for arg in args)


def findFirst(s, regexp=".*", wrap=lambda x: x):
    res = re.search(regexp, s)
    if res is None:
        return None
    return wrap(res.group(0))


class Page:
    def __init__(self, baseUrl, params: dict = None) -> None:
        self.baseUrl = baseUrl
        self.params = params
        self.html = None
        self.soup = None
        self.isRefreshed = False

    def refreshPageHtml(self) -> None:
        if not self.isRefreshed:
            headers = {
                'User-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            }

            proxies_list = ["109.194.22.61", "83.167.20.14", "195.133.45.149", "62.33.207.202", "93.157.248.108",
                            "81.23.193.94"]

            proxies = {
                'http': random.choice(proxies_list)
            }

            self.html = requests.get(self.baseUrl, params=self.params, headers=headers, proxies=proxies).text
            self.soup = BeautifulSoup(self.html, features="html.parser")
            self.isRefreshed = True

    def commonCode(self, func, tagName, className=None):
        if className is None:
            return func(tagName)
        return func(tagName, class_=className)

    # TODO common code!!!
    def getAllElementsByTag(self, tagName, className=None):
        self.refreshPageHtml()
        return self.commonCode(self.soup.findAll, tagName, className)

    def getElementByTag(self, tagName, className=None):
        self.refreshPageHtml()
        return self.commonCode(self.soup.find, tagName, className)

    def getAllElementsByDivAndClass(self, className) -> str:
        return self.getAllElementsByTag('div', className)

    def getElementByDivAndClass(self, className) -> str:
        return self.getElementByTag('div', className)

    def getElementById(self, tagName, id):
        return self.soup.find(tagName, id=id)


class MainPage(Page):

    def __init__(self, baseUrl, params: dict = None) -> None:
        super().__init__(baseUrl, params)
        self.pageIdentifier = "p"
        self.lastCrawlerPage = 0
        self.maxPage = int(findFirst(self.getElementByTag("a", "last-p").get("href"), "[0-9]+"))

    def hasUnreadPage(self):
        self.refreshPageHtml()
        self.params.setdefault(self.pageIdentifier, 1)
        if self.maxPage <= self.lastCrawlerPage:
            return False

        return True

    def nextPage(self):
        self.params[self.pageIdentifier] = self.params.get(self.pageIdentifier) + 1
        self.lastCrawlerPage += 1
        self.isRefreshed = False
        print("page", self.params[self.pageIdentifier] - 1)

    def setPageIdentifier(self, v):
        self.pageIdentifier = v

    def getAllArticlesPages(self, articleClassName, articleTag="a"):
        self.refreshPageHtml()
        return list(map(
            lambda raw: ArticlePage(slash_join(BASEURL, raw.get("href"))),
            self.getAllElementsByTag(articleTag, articleClassName)))


class ArticlePage(Page):
    def __init__(self, baseUrl, params: dict = None) -> None:
        super().__init__(baseUrl, params)

    def getDivKeyValueStatistics(self, classNameKey, classNameValue, model: dict):
        characteristics = self.getAllElementsByDivAndClass(classNameKey)

        for s in characteristics:
            for token in s.strings:
                if token.strip('\n\r\t ') in model.keys():
                    node = token.parent
                    while node.find('div', class_=classNameValue) is None:
                        node = node.parent
                    model[token.strip('\n\r\t ')] = node.find('div', class_=classNameValue).contents[0].strip()
                    break
        return

    def getDivContents(self, divClassName) -> list:
        if self.getElementByDivAndClass(divClassName) is None:
            return [""]

        res = []
        for el in self.getElementByDivAndClass(divClassName).children:
            if el.string is not None and not el.string.isspace():
                res.append(el.string.strip())
        return res

    def getImgLink(self, id):
        imgToken = self.getElementById("img", id)
        if imgToken is None:
            return ""
        return imgToken.get("src")


def validateBike(bike):
    # checking for null
    if bike["Вес"] is None:
        return False

    if bike["Название"] is None:
        return False
    return True


def parseArticle(article):
    global dataModels

    article.refreshPageHtml()
    bike = dict.fromkeys(
        ["Вес", "Тип вилки", "Тип рамы", "Тип велосипеда", "Тормоза", "Название", "Ссылка на фото",
         "Популярность посещения сайта (в неделю)", "Цена", "Скидка"])
    article.getDivKeyValueStatistics("ah-card-spec__item", "ah-card-spec__value", bike)

    bike["Название"] = article.getDivContents("ah-card__title")[0]

    bike["Тип велосипеда"] = "->".join(article.getDivContents("ah-breadcrumbs")[-2:])

    bike["Ссылка на фото"] = article.getImgLink("mainfotoVelo_src")

    bike["Популярность посещения сайта (в неделю)"] = findFirst(
       article.getDivContents("ah-card-info__col-actions-text")[0],
        "[0-9]+")

    bike["Скидка"] = findFirst(article.getDivContents("ah-card-info__discount")[0],
                               "[0-9]+", lambda s: "0." + s)
    bike["Цена"] = findFirst(
        article.getDivContents("ah-card-info__price")[0],
        wrap=lambda l: l.replace(" ", "").replace("₽", ""))

    if validateBike(bike):
        dataModels.append(bike)


def writeToCsv(file, d: list):
    w = csv.DictWriter(file, d[0].keys())
    w.writeheader()

    w.writerows(d)


def initPool(d):
    global dataModels
    dataModels = d


# (1) https://www.velosklad.ru/velosipedy/poisk/ - 1205 pages, about 2 hours of work
# (2) https://www.velosklad.ru/velosipedy/type/gruzovyie/ - 59 pages (recommended for checking)
# - each page has max 21 objects
# - (2) has 78 seconds in sequential mode, in parallel ~20 seconds
def main():
    mainPage = MainPage("https://www.velosklad.ru/velosipedy/poisk/", dict(p=1))
    dataModels = multiprocessing.Manager().list()

    with Pool(initializer=initPool, initargs=(dataModels, )) as pool:
        while mainPage.hasUnreadPage():
            articles = mainPage.getAllArticlesPages(articleClassName="ah-products-item__name")
            pool.map(parseArticle, articles)
            mainPage.nextPage()

    print("write data!")
    print(len(dataModels))
    with open('bikesdata1.csv', 'w', newline='', encoding="utf-8") as f:
        writeToCsv(f, dataModels)


if __name__ == '__main__':
    main()