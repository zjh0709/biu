from aip import AipNlp
import re


class BaiduNlp(object):
    @property
    def cleaner(self):
        dirty = "|".join(
            [u"\ufffd", u"\u30fb", u"\xe9", u"\xa0"])
        cleaner_ = re.compile("|".join(dirty))
        return cleaner_

    @property
    def nlp(self) -> AipNlp:
        __APP_ID__ = "10261696"
        __API_KEY__ = "tj6EQGQ5HKvLnbYlVvktzb9T"
        __SECRET_KEY__ = 'IsFkaIo5YNFDX7AUxWxPQ6DQ667Rd5jX'
        nlp_ = AipNlp(__APP_ID__, __API_KEY__, __SECRET_KEY__)
        return nlp_

    @property
    def patt(self):
        # filter not chinese and eng
        return re.compile(u"[^a-zA-Z\u4e00-\u9fa5\s]+")

    def keyword(self, title=None, content=None):
        pass

    def word(self, text=None) -> set:
        text = self.cleaner.sub("", text.strip())
        word_ = []
        for d in self.nlp.lexer(text).get("items", []):
            word_.append(d.get("item", ""))
            word_.extend(d.get("basic_words", []))
            word_.append(d.get("formal", ""))
        word_ = map(lambda x: self.patt.sub("", x.strip()), word_)
        word_ = set(filter(lambda x: len(x) > 1, word_))
        return word_


if __name__ == "__main__":
    baidu_nlp = BaiduNlp()
    s = """事件:根据近期与经销商交流了解到的情况,公司产品在春节期间动销良好,终端需求较旺,验证公司年报数据显示的快速增长。16年公司营业收入同比+23.6%,归母净利润同比+50.4%。16年下半年开始公司销售明显放量,
    未来公司有望进入加速成长期。    受益消费升级+全国性扩张,调味品业务发展迅猛。千禾产品定位中高端,突出零添加健康理念。公司抓住消费升级趋势,抢占中高端市场,实现快速放量,
    16年调味品业务收入同比+40%。产品方面:公司聚焦酱油、食醋、料酒三大核心品类,酱油和食醋分别实现收入3.7亿元、1.2亿元,同比+43%、+30%,料酒规模较小。公司不断完善产品系列,
    推出有机、头道原香、窖醋等差异化产品满足消费者需求,同时推动产品结构进一步优化,实现毛利率稳步提升。市场拓展方面:公司继续巩固在西南地区传统优势,16年收入同比+23.4%,保持较快增速。在开拓全国市场上,
    公司采取“高举高打”方式重点突破北上深等一线城市以树立品牌形象,同时全面铺开省会城市及重要地级市的营销网络建设。公司经销商数量从15年底的354家增加至2016年底的800家,
    营销网络覆盖面和深入度极大提高。13年开始突破的华东市场在经历战略性亏损后顺利实现盈利,收入同比+37.2%;16年下半年又重点进入以北京为代表的华北市场,未来公司有望将以点突破的成功模式复制到更多市场,
    推动公司调味品业务在全国市场遍地开花。公司另一项重要业务焦糖色收入同比下降3.8%,主因是原主要客户海天等调味品企业产品结构升级,减少对焦糖色的使用和采购。未来公司将积极拓展焦糖色的下游应用,巩固国内领先地位。    
    毛利率持续提升,费用投放较为激进,盈利能力进一步增强。受益毛利率较高的调味品业务收入占比提高,16年公司整体毛利率显著提升,达41%,同比提升3.8pct。销售费用投放较为激进,绝对额上同比+49.8%,
    销售费用率同比增加3.5pct,主要是公司大举开拓全国市场,销售人员增加,同时加大媒体投放和市场推广力度;管理费用率同比下降0.72pct,体现公司高效管理效率;上市募集资金偿还银行贷款,
    财务费用由正转负。整体三费率较同期提升1.6pct,考虑到公司正处在市场快速开拓期,短期内较高费用投放也属合理。净利率小幅提升,盈利能力进一步增强。    计划募资扩建25万吨产能,公司志在高远。公司目前拥有15万吨的产能,
    产能利用率已达95%。IPO新建项目预计将在18年完全达产,届时将达到20万吨产能规模,可满足当前市场需求。由于公司采用高盐稀态酿造工艺酿制酱油,发酵周期长,影响产能释放。考虑到未来强烈的市场需求,
    产能限制将成为公司业绩持续高增长最大阻碍。从未来战略角度考虑,公司此次计划发行可转债募资不超过3.56亿元,扩建年产25万吨酿造酱油、食醋生产线,将有效突破产能瓶颈,巩固和提升公司市场领先地位,彰显公司远见。    
    盈利预测与投资建议。预计公司2017-2019年收入分别为9.6亿元、12.0亿元、14.8亿元,归母净利润分别为1.39亿元、1.87亿元、2.52亿元,暂不考虑股本摊薄,对应动态PE分别为43倍、32倍和24倍,
    维持“增持”评级。    风险提示:原材料价格变动风险,产品销售或不及预期。 """
    print(baidu_nlp.word(s))
