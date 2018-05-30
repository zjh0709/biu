from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from urllib.parse import quote

user_agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 " \
             "Safari/537.36HH=14Runtime=lfhglmomfihibpkplgodklnokgbeajfhALICDN/ DOL/HELLO_GWF_s_8195_r2x9ak474125_656 "


def get_phantomjs_driver():
    capabilities = webdriver.DesiredCapabilities.PHANTOMJS
    capabilities["phantomjs.page.settings.resourceTimeout"] = 10000
    capabilities["phantomjs.page.settings.loadImages"] = False
    capabilities["phantomjs.page.settings.userAgent"] = "Opera/9.80 (WindowsNT6.1; U; en) Presto/2.8.131 Version/11.11"
    driver_ = webdriver.PhantomJS(executable_path="C:/Program Files/phantomjs-2.1.1-windows/bin/phantomjs.exe",
                                  desired_capabilities=capabilities)
    return driver_


def get_chrome_driver():
    capabilities = webdriver.DesiredCapabilities.CHROME
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument('user-agent="{}"'.format(user_agent))
    driver_ = webdriver.Chrome(executable_path="chromedriver",
                               port=9515,
                               desired_capabilities=capabilities,
                               chrome_options=chrome_options)
    return driver_


def request(driver_: WebDriver, url_: str):
    driver_.get(url_)
    return driver_.page_source


if __name__ == '__main__':
    driver = get_chrome_driver()
    url = "https://www.qichacha.com/search?key=上海联东地中海国际船舶代理有限公司"
    url = quote(url, safe='/:?=')
    print(request(driver, url))
    driver.quit()
