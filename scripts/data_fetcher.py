import sqlite3
from datetime import datetime, timedelta, date
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Collector:
    def __init__(self, db_path):
        # 初始化并连接到数据库
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # 确保表结构存在
        self.create_tables()

    def create_tables(self):
        # 创建记录每日、每月和每年用电量的表格
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS power_usage (
            date TEXT PRIMARY KEY,
            daily_usage REAL,
            monthly_usage REAL,
            yearly_usage REAL
        )
        ''')

        # 创建记录每次剩余电量快照的表
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS power_snapshot (
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            remaining_power REAL
        )
        ''')

        # 创建记录每半小时用电量的表
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS half_hourly_usage (
            timestamp DATETIME PRIMARY KEY,
            half_hourly_usage REAL
        )
        ''')

        # 创建记录充值电费剩余的表
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS remaining_balance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            balance REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        self.conn.commit()

    def log_power_usage(self, remaining_power):
        today_date = date.today().isoformat()
        current_month = date.today().strftime('%Y-%m')
        current_year = date.today().strftime('%Y')

        # 获取上次记录的剩余电量
        self.cursor.execute("SELECT remaining_power FROM power_snapshot ORDER BY timestamp DESC LIMIT 1")
        last_record = self.cursor.fetchone()

        if last_record:
            last_remaining_power = last_record[0]
            usage = max(0, last_remaining_power - remaining_power)
        else:
            usage = 0

        # 插入当前剩余电量快照
        self.cursor.execute("INSERT INTO power_snapshot (remaining_power) VALUES (?)", (remaining_power,))
        self.conn.commit()

        # 更新每日、每月和每年用电量
        self.cursor.execute("SELECT daily_usage, monthly_usage, yearly_usage FROM power_usage WHERE date = ?", (today_date,))
        today_record = self.cursor.fetchone()

        if today_record:
            new_daily_usage = today_record[0] + usage
            new_monthly_usage = today_record[1] + usage
            new_yearly_usage = today_record[2] + usage
            self.cursor.execute("""
                UPDATE power_usage 
                SET daily_usage = ?, monthly_usage = ?, yearly_usage = ? 
                WHERE date = ?
            """, (new_daily_usage, new_monthly_usage, new_yearly_usage, today_date))
        else:
            # 新的一天时，重新查询当月和当年累计值
            self.cursor.execute("SELECT SUM(daily_usage) FROM power_usage WHERE strftime('%Y-%m', date) = ?", (current_month,))
            monthly_total = self.cursor.fetchone()[0] or 0

            self.cursor.execute("SELECT SUM(daily_usage) FROM power_usage WHERE strftime('%Y', date) = ?", (current_year,))
            yearly_total = self.cursor.fetchone()[0] or 0

            new_daily_usage = usage
            new_monthly_usage = monthly_total + usage
            new_yearly_usage = yearly_total + usage

            self.cursor.execute("""
                INSERT INTO power_usage (date, daily_usage, monthly_usage, yearly_usage) 
                VALUES (?, ?, ?, ?)
            """, (today_date, new_daily_usage, new_monthly_usage, new_yearly_usage))

        self.conn.commit()
        logging.info(f"日期: {today_date}, 本次用电量 = {usage} kWh, 今日累计 = {new_daily_usage} kWh, 本月累计 = {new_monthly_usage} kWh, 本年累计 = {new_yearly_usage} kWh")

    def log_half_hourly_usage(self, remaining_power):
        current_time = datetime.now()

        # 获取上次记录的剩余电量
        self.cursor.execute("SELECT remaining_power FROM power_snapshot ORDER BY timestamp DESC LIMIT 1")
        last_record = self.cursor.fetchone()

        if last_record:
            last_remaining_power = last_record[0]
            usage = max(0, last_remaining_power - remaining_power)
            self.cursor.execute("INSERT INTO half_hourly_usage (timestamp, half_hourly_usage) VALUES (?, ?)", (current_time, usage))
            self.conn.commit()
            logging.info(f"半小时用电量记录: 时间: {current_time}, 用电量: {usage} kWh")
        else:
            self.cursor.execute("INSERT INTO power_snapshot (remaining_power) VALUES (?)", (remaining_power,))
            self.conn.commit()
            logging.info(f"初始化半小时用电量记录: 时间: {current_time}, 剩余电量: {remaining_power} kWh")
    def log_remaining_balance(self, balance):
        # 记录当前的剩余电费
        current_time = datetime.now()
        self.cursor.execute("INSERT INTO remaining_balance (balance, timestamp) VALUES (?, ?)", (balance, current_time))
        self.conn.commit()
        logging.info(f"记录电费余额: 时间: {current_time}, 余额: {balance} CNY")

    def log(self, remaining_power):
        self.log_half_hourly_usage(remaining_power)
        self.log_power_usage(remaining_power)
        self.log_remaining_balance(remaining_power)

    def get_daily_usage_value(self):
        today_date = date.today().isoformat()
        self.cursor.execute("SELECT daily_usage FROM power_usage WHERE date = ?", (today_date,))
        result = self.cursor.fetchone()
        return result[0] if result else 0.0

    def get_monthly_usage_value(self):
        current_month = date.today().strftime('%Y-%m')
        self.cursor.execute("SELECT SUM(daily_usage) FROM power_usage WHERE strftime('%Y-%m', date) = ?", (current_month,))
        result = self.cursor.fetchone()
        return result[0] if result else 0.0

    def get_yearly_usage_value(self):
        current_year = date.today().strftime('%Y')
        self.cursor.execute("SELECT SUM(daily_usage) FROM power_usage WHERE strftime('%Y', date) = ?", (current_year,))
        result = self.cursor.fetchone()
        return result[0] if result else 0.0

    def get_half_hourly_usage_value(self):
        # 获取最近的半小时用电量
        self.cursor.execute("SELECT half_hourly_usage FROM half_hourly_usage ORDER BY timestamp DESC LIMIT 1")
        result = self.cursor.fetchone()
        return result[0] if result else 0.0

    def get_remaining_balance(self):
        # 获取最新的电费剩余量
        self.cursor.execute("SELECT balance FROM remaining_balance ORDER BY timestamp DESC LIMIT 1")
        result = self.cursor.fetchone()
        return result[0] if result else 0.0

    def close(self):
        # 关闭数据库连接
        self.conn.close()
