import logging.config
import requests
import os
import schedule
from bs4 import BeautifulSoup
import time
import logging
from dotenv import load_dotenv
from data_fetcher import Collector
from sensor_updator import HomeAssistantUploader  # 确保替换为实际模块名


    
# 加载 .env 文件
load_dotenv()

balance_url = os.getenv("BALANCE_URL")
db_name = os.getenv("DB_NAME")
# 从环境变量中获取 Home Assistant 配置
hass_url = os.getenv("HASS_URL")
hass_token = os.getenv("HASS_TOKEN")
daily_usage_entity_id = os.getenv("DAILY_USAGE_ENTITY_ID")
monthly_usage_entity_id = os.getenv("MONTHLY_USAGE_ENTITY_ID")
yearly_usage_entity_id = os.getenv("YEARLY_USAGE_ENTITY_ID")
half_hourly_usage_entity_id = os.getenv("HALF_HOURLY_USAGE_ENTITY_ID")
remaining_balance_entity_id = os.getenv("REMAINING_BALANCE_ENTITY_ID")
remaining_charge_entity_id = os.getenv("REMAINING_CHARGE_ENTITY_ID")

# 获取定时任务时间配置
half_hourly_interval = int(os.getenv("HALF_HOURLY_INTERVAL", 30))  # 默认值为30分钟
daily_upload_time = os.getenv("DAILY_UPLOAD_TIME", "00:00")  # 默认值为"00:00"
monthly_upload_time = os.getenv("MONTHLY_UPLOAD_TIME", "00:00")  # 默认值为"00:00"
yearly_upload_time = os.getenv("YEARLY_UPLOAD_TIME", "00:00")  # 默认值为"00:00"

# 检查是否成功获取到所有配置项
required_env_vars = [hass_url, hass_token, daily_usage_entity_id, monthly_usage_entity_id,
                     yearly_usage_entity_id, half_hourly_usage_entity_id, remaining_balance_entity_id]
if any(var is None for var in required_env_vars):
    raise ValueError("缺少必要的配置，请检查 .env 文件。")

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_remaining_power():
            # 发送HTTP请求并获取网页内容
    response = requests.get(balance_url)
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
                return elements.text
            else:
                logging.error(f'未找到指定的 {key} 元素')
    else:
        logging.error(f'请求失败，状态码: {response.status_code}')

def main():
    # 初始化 Collector 和 HomeAssistantUploader 实例
    collector = Collector(db_name)
    uploader = HomeAssistantUploader(collector, hass_url, hass_token)

    # 定义上传任务
    def upload_daily_usage():
        logging.info("执行每日用电量上传任务...")
        uploader.upload_daily_usage(entity_id=daily_usage_entity_id)

    def upload_monthly_usage():
        logging.info("执行每月用电量上传任务...")
        uploader.upload_monthly_usage(entity_id=monthly_usage_entity_id)

    def upload_yearly_usage():
        logging.info("执行每年用电量上传任务...")
        uploader.upload_yearly_usage(entity_id=yearly_usage_entity_id)

    def upload_half_hourly_usage():
        logging.info("执行半小时用电量上传任务...")
        uploader.upload_half_hourly_usage(entity_id=half_hourly_usage_entity_id)

    def upload_remaining_balance():
        logging.info("执行剩余电费上传任务...")
        uploader.upload_remaining_balance(entity_id=remaining_balance_entity_id)

    def upload_remaining_charge():
        logging.info("执行剩余电费上传任务...")
        uploader.upload_remaining_charge(entity_id=remaining_charge_entity_id)

    def loging():
        remaining = float(get_remaining_power())
        collector.log(remaining)

    # 使用 .env 文件中的配置来设置定时任务
    schedule.every(1).minutes.do(loging)
    schedule.every(half_hourly_interval).minutes.do(upload_remaining_charge)
    schedule.every(half_hourly_interval).minutes.do(upload_half_hourly_usage)
    schedule.every(half_hourly_interval).minutes.do(upload_remaining_balance)
    schedule.every().day.at(daily_upload_time).do(upload_daily_usage)
    schedule.every().day.at(monthly_upload_time).do(upload_monthly_usage)
    schedule.every().day.at(yearly_upload_time).do(upload_yearly_usage)

    logging.info("定时任务已启动。")

    try:
        # 循环执行定时任务
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每隔 60 秒检查一次任务
    except KeyboardInterrupt:
        logging.info("定时任务终止，正在关闭资源...")
    finally:
        collector.close()

if __name__ == "__main__":
    main()