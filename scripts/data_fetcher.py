import logging
import traceback
import requests
import sqlite3
import requests
import os
from datetime import timedelta, datetime
from bs4 import BeautifulSoup
from const import *

# 目标网页URL


class DataFetcher:
    def __init__(self):
        self._balance_list = []
        self._last_run_date = None
        self._last_day_usage = None
        self._user_id =  os.getenv("USER_ID")
        self.table_name = None
        self.connect = None
        self.db_name = "homeassistant.db"
        self.accumulate_energe = 0.0
        self.connect_user_db(self._user_id)


    def connect_user_db(self, user_id):
        """创建数据库集合，table_name = electricity_daily_usage_{user_id}
        :param user_id: 用户ID"""
        try:
            # 创建数据库
            self.connect = sqlite3.connect(self.db_name)
            self.connect.cursor()
            logging.info(f"Database of {self.db_name} created successfully.")
            try:
                # 创建表名
                self.table_name = f"daily{user_id}"
                sql = f"CREATE TABLE {self.table_name} (date DATE PRIMARY KEY NOT NULL, usage REAL NOT NULL);"
                self.connect.execute(sql)
                logging.info(f"Table {self.table_name} created successfully")
            except BaseException as e:
                logging.debug(f"Table {self.table_name} already exists: {e}")
        # 如果表已存在，则不会创建
        except BaseException as e:
            logging.debug(f"Table: {self.db_name} already exists:{e}")
        finally:
            return self.connect

    def insert_data(self, data:dict):
            # 创建索引
            try:
                sql = f"INSERT OR REPLACE INTO {self.table_name} VALUES(strftime('%Y-%m-%d','{data['date']}'),{data['usage']});"
                self.connect.execute(sql)
                self.connect.commit()
            except BaseException as e:
                logging.debug(f"Data update failed: {e}")

    def _calculate_power_usage(self):
        now = datetime.now()
        if now.hour == 1 and now.minute == 0 and (self._last_run_date is None or self._last_run_date != now.date()):
            if not self._balance_list or len(self._balance_list) < 2:
                raise ValueError("列表至少两个元素")
            first_element = self._balance_list[0]
            index = -1
            result = -1
            while result < 0:
                result = first_element - self._balance_list[index]
                index -= 1
                if abs(index) > len(self._balance_list):
                    logging.error("无法得到正确结果")
                    break
                    
            self._balance_list = None
            # 新增储存用电量
            self.save_usage_data(self._user_id, datetime.now(), result)

    def fetch(self):
        """the entry, only retry logic here """
        try:
            return self._fetch()
        except Exception as e:
            traceback.print_exc()
            logging.error(f"{e}")

    def _requst_date(self):
        # 发送HTTP请求并获取网页内容
        response = requests.get(BALANCE_URL)
        dates = {
            # 'user_id': 'body > div:nth-of-type(2) > div:nth-of-type(2) > div:nth-of-type(2) > span:nth-of-type(1)',
            'balance' : 'body > div:nth-of-type(2) > div:nth-of-type(2) > div:nth-of-type(2) > label'
        }
        # 检查请求是否成功
        if response.status_code == 200:
            # 解析网页内容
            soup = BeautifulSoup(response.content, 'lxml')
            for key, xpath in dates.items():
                # 使用 BeautifulSoup 来查找特定的元素（通过 XPath 转换为 CSS 选择器）
                # 路径: /html/body/div[2]/div[2]/div[2]/label
                elements = soup.select_one(xpath)

                # 如果找到第一个元素，打印其内容
                if elements:
                    logging.info(f'抓取到的 {key} 数据: {elements.text}')
                    self._balance_list.append(float(elements.text))
                    return elements.text
                else:
                    logging.error(f'未找到指定的 {key} 元素')
        else:
            logging.error(f'请求失败，状态码: {response.status_code}')

    def _fetch(self):
        """main logic here"""
        try:
            self._calculate_power_usage()
            user_id_list = self._get_user_ids()
            logging.info(f"There are {len(user_id_list)} users in total, there user_id is: {user_id_list}")
            balance_list = self._get_electric_balances(user_id_list)  #
            last_daily_date_list, last_daily_usage_list, yearly_charge_list, yearly_usage_list, month_list, month_usage_list, month_charge_list  = self._get_other_data(user_id_list)
            return user_id_list, balance_list, last_daily_date_list, last_daily_usage_list, yearly_charge_list, yearly_usage_list, month_list, month_usage_list, month_charge_list 
        finally:
            logging.error("Some Thing")

        
    def _get_electric_balances(self,user_id_list):
        balance_list = []
        # switch to electricity charge balance page
        for i in range(1, len(user_id_list) + 1):
            balance = self._get_eletric_balance()
            if (balance is None):
                logging.info(f"Get electricity charge balance for {user_id_list[i - 1]} failed, Pass.")
            else:
                logging.info(
                    f"Get electricity charge balance for {user_id_list[i - 1]} successfully, balance is {balance} CNY.")
            balance_list.append(balance)

        return balance_list

    def _get_other_data(self, user_id_list):
        last_daily_date_list = []
        last_daily_usage_list = []
        yearly_usage_list = []
        yearly_charge_list = []
        month_list = []
        month_charge_list = []
        month_usage_list = []
        # swithc to electricity usage page

        # get data for each user id
        for i in range(1, len(user_id_list) + 1):

            yearly_usage, yearly_charge = self._get_yearly_data()

            if yearly_usage is None:
                logging.error(f"Get year power usage for {user_id_list[i - 1]} failed, pass")
            else:
                logging.info(
                    f"Get year power usage for {user_id_list[i - 1]} successfully, usage is {yearly_usage} kwh")
            if yearly_charge is None:
                logging.error(f"Get year power charge for {user_id_list[i - 1]} failed, pass")
            else:
                logging.info(
                    f"Get year power charge for {user_id_list[i - 1]} successfully, yealrly charge is {yearly_charge} CNY")

            # get month usage
            month, month_usage, month_charge = self._get_month_usage()
            if month is None:
                logging.error(f"Get month power usage for {user_id_list[i - 1]} failed, pass")
            else:
                for m in range(len(month)):
                    logging.info(
                        f"Get month power charge for {user_id_list[i - 1]} successfully, {month[m]} usage is {month_usage[m]} KWh, charge is {month_charge[m]} CNY.")
            # get yesterday usage
            last_daily_datetime, last_daily_usage = self._get_yesterday_usage()


            if last_daily_usage is None:
                logging.error(f"Get daily power consumption for {user_id_list[i - 1]} failed, pass")
            else:
                logging.info(
                    f"Get daily power consumption for {user_id_list[i - 1]} successfully, , {last_daily_datetime} usage is {last_daily_usage} kwh.")

            last_daily_date_list.append(last_daily_datetime)
            last_daily_usage_list.append(last_daily_usage)
            yearly_charge_list.append(yearly_charge)
            yearly_usage_list.append(yearly_usage)
            if month:
                month_list.append(month[-1])
            else:
                month_list.append(None)
            if month_charge:
                month_charge_list.append(month_charge[-1])
            else:
                month_charge_list.append(None)
            if month_usage:
                month_usage_list.append(month_usage[-1])
            else:
                month_usage_list.append(None)

        return last_daily_date_list, last_daily_usage_list, yearly_charge_list, yearly_usage_list, month_list, month_usage_list, month_charge_list

    def _get_user_ids(self):
        userid_list = [self._user_id]
        return userid_list

    def _get_eletric_balance(self):
        try:
            balance = self._requst_date()
            logging.info(f"获取blance: {balance}")
            return float(balance)
        except:
            return None

    def _get_yearly_data(self):
        # 获取当前日期并计算一年前的日期
        today = datetime.today()
        one_year_ago = today - timedelta(days=365)

        table_name = self.table_name
        # 构建 SQL 查询，筛选出最近一年的数据，并按年份汇总
        query = f"""
        SELECT strftime('%Y', date) AS year, SUM(usage)
        FROM {table_name}
        WHERE date >= '{one_year_ago.strftime('%Y-%m-%d')}'
        GROUP BY year
        ORDER BY year ASC
        """
        # 执行查询
        cursor = self.connect.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) < 1:
            return None, None
        yearly_usage = result[-1][1]
        yearly_charge = yearly_usage

        return yearly_usage, yearly_charge

    def _get_yesterday_usage(self):
        """获取最近一次用电量"""
        table_name = self.table_name
        # 构建 SQL 查询语句，按日期降序排列，并取出最新的一天数据
        query = f"""
        SELECT date, usage
        FROM {table_name}
        ORDER BY date DESC
        LIMIT 1
        """
        self.connect.cursor().execute(query)
        result = self.connect.cursor().fetchone()
        if result is None or len(result) < 1:
            # return "2024-10-5", 10
            return None, None
        
        return result[0], float(result[1])

    def _get_month_usage(self):
        """获取每月用电量"""

        # 构建 SQL 查询，按月份汇总数据
        table_name = self.table_name
        # 获取当前日期并计算一年前的日期
        today = datetime.today()
        one_year_ago = today - timedelta(days=365)
        
        # 构建 SQL 查询，按月份汇总数据，并筛选出最近一年的数据
        query = f"""
        SELECT strftime('%Y-%m', date) AS month, SUM(usage)
        FROM {table_name}
        WHERE date >= '{one_year_ago.strftime('%Y-%m-%d')}'
        GROUP BY month
        ORDER BY month ASC
        """
        # 执行查询
        cursor = self.connect.cursor()
        cursor.execute(query)
        month_element = cursor.fetchall()
        try:
            # wait for month displayed
            # 将每月的用电量保存为List
            month = []
            usage = []
            charge = []
            for i in range(len(month_element)):
                month.append(month_element[i][0])
                usage.append(month_element[i][1])
                charge.append(month_element[i][1])
            return month, usage, charge
        except:
            return None,None,None

    # 增加储存用电量的到mongodb的函数
    def save_usage_data(self, user_id, day, usage):
        """储存指定天数的用电量"""
        # 连接数据库集合
        self.connect_user_db(user_id)

        # 将用电量保存为字典
        dic = {'date': day, 'usage': float(usage)}
        # 插入到数据库
        try:
            self.insert_data(dic)
            logging.info(f"The electricity consumption of {usage}KWh on {day} has been successfully deposited into the database")
        except Exception as e:
            logging.debug(f"The electricity consumption of {day} failed to save to the database, which may already exist: {str(e)}")

        self.connect.close()



if __name__ == "__main__":
    fetcher = DataFetcher()
    fetcher.fetch()
