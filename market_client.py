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
    def __init__(
        self, md_front, broker_id, user_id, password, file, end_time="09:10", idd=0
    ):
        self.md_front = md_front
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password
        self.id = idd
        self.end_time = datetime.strptime(
            f"{datetime.today().date()} {end_time}", "%Y-%m-%d %H:%M"
        )
        self.date = datetime.today().date()
        self.file = file
        self.writer = csv.writer(file)

    def login(self, id=0):
        """
        登录行情、交易
        """
        login = ReqUserLoginField(
            BrokerID=self.broker_id,
            UserID=self.user_id,
            Password=self.password,
            # TradingDay="20240419",
            # MacAddress=quota_front,
        )
        return self.ReqUserLogin(login, id)

    def logout(self):
        """
        登出
        """
        login = ReqUserLoginField(
            BrokerID=self.broker_id,
            UserID=self.user_id,
            Password=self.password,
            # MacAddress=quota_front,
        )
        self.ReqUserLogout(login, 0)

    def subscribe(self, codes):
        """
        订阅合约代码
        """
        self.subid = codes
        return self.SubscribeMarketData(codes)

    def unsubscribe(self, codes):
        """
        取消订阅
        """
        return self.UnSubscribeMarketData(codes)

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
            self.file.close()
            self.unsubscribe(self.subid)
            self.logout()
            self.Release()
        tim = datetime.now()
        tim = ":".join(
            [
                str(tim.hour),
                str(tim.minute),
                "".join([str(tim.second).zfill(2), str(tim.microsecond)[:3]]),
            ]
        )
        change_tim = "".join(
            [
                str(pDepthMarketData.UpdateTime),
                str(pDepthMarketData.UpdateMillisec),
            ]
        )
        row = [
            tim,
            change_tim,
            str(pDepthMarketData.InstrumentID),
            str(pDepthMarketData.LastPrice),
            str(self.id),
        ]
        #
        self.writer.writerow(row)
        print("write a row")


def main(quota_front, quotaid, subid, file, endtime="09:10"):
    broker_id = config["broker_id"]
    user_id = config["investor_id"]
    password = config["password"]
    md_api1 = Client(quota_front, broker_id, user_id, password, file, endtime, quotaid)
    md_api1.subid = subid
    md_api1.Create()
    md_api1.RegisterFront(quota_front)
    md_api1.Init()
    md_api1.login()
    md_api1.SubscribeMarketData(subid)
    return md_api1


if __name__ == "__main__":
    end_time = "13:42"
    subid = ["au2406", "ag2406", "sc2406"]
    # subid = ["au2406"]
    source1 = "tcp://101.226.253.177:61213"
    source2 = "tcp://140.207.230.97:61213"
    source3 = "tcp://192.168.112.29:61213"
    source4 = "tcp://tcp://10.10.16.29:61213"
    today = datetime.today().strftime("%Y-%m-%d")
    file = open(f"./data/{today}_result.csv", "+a", newline="")
    md_api1 = main(source1, "tele1", subid, file, end_time)
    md_api2 = main(source2, "unicom1", subid, file, end_time)
    md_api3 = main(source2, "gigabit", subid, file, end_time)
    md_api4 = main(source2, "10_gigabit", subid, file, end_time)
    # md_api3=main(source3, "allday", subid,file, end_time)
    md_api1.Join()
    md_api2.Join()
    md_api3.Join()
    md_api4.Join()
