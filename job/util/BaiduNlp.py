from aip import AipNlp
import re
from ..config import BAIDU_APP_ID, BAIDU_API_KEY, BAIDU_SECRET_KEY


class BaiduNlp(object):
    @property
    def nlp(self) -> AipNlp:
        nlp_ = AipNlp(BAIDU_APP_ID, BAIDU_API_KEY, BAIDU_SECRET_KEY)
        return nlp_

    @property
    def patt(self):
        # filter not chinese and eng
        return re.compile(u"[^a-zA-Z\u4e00-\u9fa5\s]+")

    def keyword(self, title=None, content=None):
        pass

    def word(self, text:str=None) -> set:
        text = text.strip().encode("utf-8", "ignore").decode("utf-8")
        word_ = []
        for d in self.nlp.lexer(text).get("items", []):
            # only keep pos is n or have n or null
            if "n" not in d.get("pos", "n"):
                continue
            word_.append(d.get("item", ""))
            # word_.extend(d.get("basic_words", []))
            # word_.append(d.get("formal", ""))
        word_ = map(lambda x: self.patt.sub("", x.strip()), set(word_))
        word_ = set(filter(lambda x: len(x) > 1, word_))
        return word_

