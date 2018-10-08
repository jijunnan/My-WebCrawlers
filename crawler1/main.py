# 本爬虫程序主要用于从中金所官网爬取个期货公司国债期货合约持仓数据
# 作者： 季俊男
# 更新日期：2018/9/25

import requests
from bs4 import BeautifulSoup
import datetime


def sort_long_short_net(e: str):
    if e == "买":
        res = 1
    elif e == "净":
        res = 0
    elif e == "卖":
        res = -1
    else:
        raise TypeError("错误的参数值e")
    return res


class PositionScrap(object):
    """类PositionScrap用于从中金所官网上爬取期货合约的持仓数量信息，并提供计算净持仓的方法"""
    def __init__(self, dt: datetime.date, contract: str):
        self.dt = dt
        self.sdt = dt.strftime("%Y%m%d")
        self.contract = contract
        self.url = self.get_url()
        self.soup = self.get_soup()
        self.long_position = self.get_data_by_value(1)
        self.short_position = self.get_data_by_value(2)
        self.inti_long_position = self.get_intigrated_position(1)
        self.inti_short_position = self.get_intigrated_position(2)

    def get_url(self):
        """由合约代码与日期生成get请求的目标url"""
        ym = self.sdt[0:6]
        d = self.sdt[6:8]
        url = r"http://www.cffex.com.cn/sj/ccpm/{}/{}/{}.xml".format(ym, d, self.contract)
        return url

    def get_soup(self):
        """读取目标url的xml内容并以beautifulsoup进行解析"""
        res = requests.get(self.url)
        soup = BeautifulSoup(res.content, "lxml")
        return soup

    def get_data_by_value(self, value):
        """根据data标签的value值来确定买单持仓（value=1）还是卖单持仓（value=2）以及成交量（value=0）
        以二维列表形式返还结果"""
        data = {}
        for tag in self.soup.find_all("data", {"value": value}):
            try:
                data[tag.instrumentid.string][tag.shortname.string] = int(tag.volume.string)
            except KeyError as e:
                data[tag.instrumentid.string] = {}
                data[tag.instrumentid.string][tag.shortname.string] = int(tag.volume.string)
        return data

    def get_intigrated_position(self, value):
        """将不同合约的持仓数据加总，无视合约月份的区别"""
        if value == 1:
            contract_ym = self.long_position.keys()
            position = self.long_position
        elif value == 2:
            contract_ym = self.short_position.keys()
            position = self.short_position
        else:
            raise TypeError("错误的value参数")
        inti_position = {}
        for cym in contract_ym:
            for name in position[cym].keys():
                try:
                    inti_position[name] += position[cym][name]
                except KeyError as e:
                    inti_position[name] = position[cym][name]
        return inti_position

    def net_position(self, mode=0):
        """计算各期货公司的净持仓，由于中金所只公布买卖持仓前二十的期货公司，无买卖双边持仓数据的期货公司只计算单边持仓，
        买单持仓为正数，卖单持仓为负数。模式0（mode=0)为将不同月份合约合并起来计算，模式1（mode=1）则将不同月份合约分开
        计算并只计算同时拥有买卖双边持仓的期货公司的净持仓"""
        net =[]
        contract_ym = self.long_position.keys()
        if mode == 0:
            long_name = set(self.inti_long_position.keys())
            short_name = set(self.inti_short_position.keys())
            inter_name = long_name & short_name
            diff_long_name = long_name - inter_name
            diff_short_name = short_name - inter_name
            for name in inter_name:
                net.append((name, self.inti_long_position[name] - self.inti_short_position[name], "净"))
            for name in diff_long_name:
                net.append((name, self.inti_long_position[name], "买"))
            for name in diff_short_name:
                net.append((name, -self.inti_short_position[name], "卖"))
            net.sort(key=lambda x: (sort_long_short_net(x[2]), x[1]), reverse=True)

        elif mode == 1:
            for cym in contract_ym:
                long_name = set(self.long_position[cym].keys())
                short_name = set(self.short_position[cym].keys())
                name = long_name.intersection(short_name)
                for n in name:
                    net.append((cym, n, self.long_position[cym][n] - self.short_position[cym][n]))
            net.sort(key=lambda x: (x[0], x[2]),reverse = True)
        return net


if __name__ == "__main__":
    dt = datetime.date(2018, 8, 17)
    for contract in ["TS"]:
        print()
        lll = PositionScrap(dt,contract)
        for n in lll.net_position(1):
            print()
            for m in n:
                print(m, end="   ")

