import requests
from bs4 import BeautifulSoup
import re
import logging


def get_topic_from_sina(code: str, page: int = 1) -> tuple:
    topic_url = "http://vip.stock.finance.sina.com.cn/q/go.php/vReport_List/kind/search/index.phtml?symbol={" \
                "}&t1=all&p={}".format(code, page)
    url_expr = "vip.stock.finance.sina.com.cn/q/go.php/vReport_Show/kind/search/rptid"
    r = requests.get(topic_url)
    r.encoding = 'gb2312'
    soup = BeautifulSoup(r.text, "html.parser")
    links_div = soup.find_all("a", href=re.compile(url_expr))
    links = []
    for link in links_div:
        link = {"url": link.get("href"),
                "title": link.get("title"),
                "code": code}
        links.append(link)
    page_buttons = soup.find_all("a", onclick=re.compile("set_page_num"))
    max_page = 1
    if page_buttons:
        last_onclick = page_buttons[-1].get("onclick")
        re_page_num = re.compile("(?<=set_page_num\(')(.*)?(?='\))")
        re_result = re_page_num.search(last_onclick)
        if re_result:
            max_page = int(re_result.group())
    return links, max_page


def get_document_from_sina(url: str) -> dict:
    r = requests.get(url)
    r.encoding = 'gb2312'
    ret = {}
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        content_select = soup.find("div", class_="content")
        if content_select:
            title_select = content_select.find("h1")
            if title_select:
                ret.setdefault("title", title_select.text)
            creab_select = content_select.find("div", class_="creab")
            if creab_select:
                ret.setdefault("span", [e.text for
                                        e in creab_select.findAll("span")])
            document_select = content_select.find("div", class_="blk_container")
            if document_select:
                ret.setdefault("content", document_select.text.strip())
    except Exception as e:
        logging.warning(e)
    return ret


if __name__ == '__main__':
    t = get_document_from_sina(
        "http://vip.stock.finance.sina.com.cn/q/go.php/vReport_Show/kind/search/rptid/4184453/index.phtml")
    print(t)
