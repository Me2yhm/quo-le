import sys
import math
import csv
import time
from datetime import datetime
import json
from ctpwrapper import MdApiPy, MdApi
from ctpwrapper.ApiStructure import ReqUserLoginField

config_file = "config.json"
with open(config_file, "r") as file:
    config = json.load(file)


def signal_handler(signal, frame):
    print("Caught Ctrl+C / SIGINT signal")
    # 在这里添加你想要做的清理操作
    # 例如停止子进程，关闭文件等
    # ...
    # 退出程序的代码
    sys.exit(0)


def timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")


def get_current_time():
    return datetime.now()


def check_end_time(end_time):
    current_time = get_current_time()
    return current_time >= end_time


def get_timestamp(timstamp):
    t = (
        datetime.strptime(
            f"{datetime.today().date()} {timstamp}", "%Y-%m-%d %H:%M:%f"
        ).timestamp()
        * 1000
    )
    return str(t).split(".")[0]


class Client(MdApiPy):
    def __init__(self, md_front, broker_id, user_id, password, end_time="09:10", idd=0):
        self.md_front = md_front
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password
        self.id = idd
        self.end_time = datetime.strptime(
            f"{datetime.today().date()} {end_time}", "%Y-%m-%d %H:%M"
        )
        self.date = datetime.today().date()

    def login(self, id=0):
        """
        登录行情、交易
        """
        login = ReqUserLoginField(
            BrokerID=broker_id,
            UserID=user_id,
            Password=password,
            # TradingDay="20240419",
            # MacAddress=quota_front,
        )
        return self.ReqUserLogin(login, id)

    def logout(self):
        """
        登出
        """
        login = ReqUserLoginField(
            BrokerID=broker_id,
            UserID=user_id,
            Password=password,
            # MacAddress=quota_front,
        )
        self.ReqUserLogout(login, 0)

    def subscribe(self, codes):
        """
        订阅合约代码
        """
        return self.SubscribeMarketData(codes)

    def unsubscribe(self, codes):
        """
        取消订阅
        """
        pass

    def OnRspSubMarketData(
        self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast
    ) -> None:
        """
        订阅行情应答
        :param pSpecificInstrument:
        :param pRspInfo:
        :param nRequestID:
        :param bIsLast:
        :return:
        """

        # print(
        #     f"Time: {timestamp_to_datetime(time.time())},InstrumentID={pSpecificInstrument.InstrumentID},{nRequestID}"
        # )
        pass

    def OnRtnDepthMarketData(self, pDepthMarketData) -> None:
        """
        深度行情通知
        :param pDepthMarketData:
        :return:
        """
        if check_end_time(self.end_time):
            self.Release()
        # tim = timestamp_to_datetime(time.time())
        tim = math.floor(time.time() * 1000)
        print(pDepthMarketData.TradingDay, pDepthMarketData.UpdateTime)
        timstamp = "".join(
            [str(pDepthMarketData.UpdateTime), str(pDepthMarketData.UpdateMillisec)]
        )
        chang_tim = get_timestamp(timstamp)
        filename = f"./result-{pDepthMarketData.InstrumentID}.csv"
        row = [
            str(tim)[-7:],
            chang_tim[-7:],
            str(pDepthMarketData.InstrumentID),
            str(pDepthMarketData.LastPrice),
            str(self.id),
        ]
        #
        with open(filename, "+a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
            print("write a row")


def main(quota_front, quotaid, subid):
    broker_id = config["broker_id"]
    user_id = config["investor_id"]
    password = config["password"]
    subid = [subid]
    md_api1 = Client(quota_front, broker_id, user_id, password, quotaid)
    md_api1.Create()
    md_api1.RegisterFront(quota_front)
    md_api1.Init()
    md_api1.login()
    md_api1.SubscribeMarketData(subid)
    md_api1.Join()


if __name__ == "__main__":
    end_time = datetime.strptime("2024-04-23 15:32", "%Y-%m-%d %H:%M")
    broker_id = config["broker_id"]
    user_id = config["investor_id"]
    password = config["password"]
    quota_front = "tcp://180.168.146.187:10212"
    appid = config["app_id"]
    trading_date = "20240422"
    auth_code = config["auth_code"]
    subid = ["au2406"]
    md_api1 = Client(quota_front, broker_id, user_id, password, "电信2")
    md_api1.Create()
    md_api1.RegisterFront(quota_front)
    md_api1.Init()
    md_api1.login()
    md_api1.SubscribeMarketData(subid)
    md_api2 = Client(quota_front, broker_id, user_id, password, "移动")
    md_api2.Create()
    md_api2.RegisterFront("tcp://218.202.237.33:10213")
    md_api2.Init()
    md_api2.login(1)
    md_api2.SubscribeMarketData(subid)
    md_api1.Join()
    md_api2.Join()
    sys.exit(0)
