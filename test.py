import requests
from bs4 import BeautifulSoup


def so(code):
    url = "https://btso.pw/search/{}".format(code)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    data_list = soup.find("div", class_="data-list")
    print(url)
    rows = explode_data_list(data_list)
    result = explode_rows(rows)
    print(result)


def explode_data_list(data_list):
    if data_list:
        rows = data_list.find_all("div", class_="row")
    else:
        rows = []
    return rows


def explode_rows(rows):
    ret = []
    for row in rows:
        tmp = {}
        tmp.setdefault("title", row.find("a").get("title"))
        tmp.setdefault("href", row.find("a").get("href"))
        tmp.setdefault("size", row.find("div", class_="size").text)
        tmp.setdefault("date", row.find("div", class_="date").text)
        ret.append(tmp)
    return ret


if __name__ == '__main__':
    so("SGA-106")
