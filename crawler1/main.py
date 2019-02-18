# 本爬虫程序主要用于从中金所官网爬取个期货公司国债期货合约持仓数据并存放至数据库
# 作者： 季俊男
# 创建日期：2018/9/25
# 更新日期：2019/2/15

import requests
from bs4 import BeautifulSoup
import datetime as dtt
import pymysql


def create_table(cur, table):
    """"创建数据库以及表"""
    sql_dts = """
    create table if not exists dts(
    `dt` date not null primary key comment '中金所交易日期',
    `seq` int not null comment '交易日期顺序'
    )ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '中金所交易日期'
    """
    sql_positions = """
    create table if not exists positions(
    `dt` date not null comment '中金所交易日期',
    `name` char(10) not null comment '期货公司',
    `contract` char(10) not null comment '期货合约',
    `volume` int default null comment '成交量',
    `volume_delta` int default null comment '成交量较上一日变化',
    `volume_rank` int default null comment '成交量排名',
    `buy` int default null comment '持买单量',
    `buy_delta` int default null comment '持买单量较上一日变化',
    `buy_rank` int default null comment '持买单量排名',
    `sell` int default null comment '持卖单量',
    `sell_delta` int default null comment '持卖单量较上一日变化',
    `sell_rank` int default null comment '持卖单量排名',
    `net` int default null comment '净持仓',
    `net_delta` int default null comment '净持仓较上一日变化',
    `net_rank` int default null comment '净持仓排名',
    constraint pk primary key(dt, name, contract)
    ) ENGINE=InnoDB DEFAULT CHARSET = utf8MB3 COMMENT = '期货公司持仓信息表'
    """
    if table is None:
        sqls = [sql_dts, sql_positions]
        for sql in sqls:
            _ = cur.execute(sql)
    else:
        _ = cur.execute("sql_{}".format(table))

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
    def __init__(self, dt: dtt.date, contract: str):
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
            net.sort(key=lambda x: (x[0], x[2]), reverse=True)
        return net


class Data(object):
    """本类用于从mysql中提取相应条件的数据"""
    def __init__(self, sql, cur, args=None):
        self.sql = sql
        self.cur = cur
        self.args = args
        self.data = Data.get_data(self)

    def __str__(self):
        return str(self.data)

    __repr__ = __str__

    def get_data(self):
        _ = self.cur.execute(self.sql, self.args)
        data = self.cur.fetchall()
        return data

    def select_col(self, col):
        return [d[col] for d in self.data]


class PositionCrawler(object):
    """用于爬取并整理向数据库内输入的数据"""
    def __init__(self, dt: dtt.date, contract: str, db, cur):
        self.dt = dt
        self.sdt = dt.strftime("%Y%m%d")
        self.contract = contract
        self.db = db
        self.cur = cur
        self.url = self.get_url()
        self.soup = self.get_soup()

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

    def get_data(self):
        """爬取数据并整理为可以写入positions表的数据结构"""
        data = {}
        for tag in self.soup.find_all("data"):
            contract = tag.instrumentid.string
            name = tag.shortname.string
            value = int(tag.attrs["value"])
            if contract not in data:
                data[contract] = {}
            if name not in data[contract]:
                data[contract][name] = [None for _ in range(12)]
            data[contract][name][3 * value: 3 * value + 3] = [int(tag.volume.string),
                                                              int(tag.varvolume.string),
                                                              int(tag.rank.string)]
        # 将data内的字典数据拓展为二维列表，该种形式的数据可以用于插入数据库表positions
        res = []
        for key_contract, value_contract in data.items():
            for key_name, value_name in value_contract.items():
                if value_name[3] is not None and value_name[6] is not None:
                    value_name[9] = value_name[3] - value_name[6]
                    value_name[10] = value_name[4] - value_name[7]
                elif value_name[3] is not None:
                    value_name[9] = value_name[3]
                    value_name[10] = value_name[4]
                elif value_name[6] is not None:
                    value_name[9] = -value_name[6]
                    value_name[10] = -value_name[7]
                r = [self.dt, key_name, key_contract, *value_name]
                res.append((r,))
        return res

    def insert_into_positions(self):
        """"向数据库future的positions表中插入数据"""
        data = self.get_data()
        sql = """insert into positions values %s"""
        try:
            self.cur.executemany(sql, data)
        except pymysql.err.IntegrityError as e:
            self.db.rollback()
            print(e)
        else:
            self.db.commit()

    def update_positions(self):
        """insert_position方法调用时不能产生net_rank字段的数据，改字段需要根据已拥有的net字段数据进行计算"""
        sql_select = """
        select dt, name, contract, net from positions 
        where dt = %s and net is not null
        order by contract asc, net desc
        """
        data_select = Data(sql_select, self.cur, (self.dt,)).data
        data_update = []
        i = 1
        contract = data_select[0][2]
        for ds in data_select:
            if ds[2] != contract:
                i = 1
                contract = ds[2]
            du = [i, ds[0], ds[1], ds[2]]
            data_update.append(du)
            i += 1
        sql_update = """
        update positions
        set net_rank = %s
        where dt = %s and name = %s and contract = %s
        """
        try:
            self.cur.executemany(sql_update, data_update)
        except pymysql.err.IntegrityError as e:
            self.db.rollback()
            print(e)
        else:
            self.db.commit()

    def insert(self):
        """将insert_into_positions方法与update_positions"""
        self.insert_into_positions()
        self.update_positions()


if __name__ == "__main__":
    db = pymysql.connect("localhost", "root", "root", charset="utf8")
    cur = db.cursor()
    cur.execute("use future_position")
    # create_table(cur, None)
    dt = dtt.date(2019, 2, 15)
    crawler = PositionCrawler(dt, "T", db, cur)
    # print(crawler.get_data())
    crawler.insert()





