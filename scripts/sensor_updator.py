import logging
import requests
# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HomeAssistantUploader:
    def __init__(self, collector, hass_url, hass_token):
        self.collector = collector
        self.hass_url = hass_url
        self.hass_token = hass_token
        self.headers = {
            'Authorization': f'Bearer {self.hass_token}',
            'Content-Type': 'application-json'
        }

    def upload_daily_usage(self, entity_id='sensor.daily_usage'):
        daily_usage = self.collector.get_daily_usage_value()
        data = {
            "state": daily_usage,
            "attributes": {
                "unit_of_measurement": "kWh",
                "friendly_name": "Daily Energy Usage",
                "device_class": "energy",
                "state_class": "measurement"
            }
        }
        self._post_to_home_assistant(entity_id, data)

    def upload_monthly_usage(self, entity_id='sensor.monthly_usage'):
        monthly_usage = self.collector.get_monthly_usage_value()
        data = {
            "state": monthly_usage,
            "attributes": {
                "unit_of_measurement": "kWh",
                "friendly_name": "Monthly Energy Usage",
                "device_class": "energy",
                "state_class": "measurement"
            }
        }
        self._post_to_home_assistant(entity_id, data)

    def upload_yearly_usage(self, entity_id='sensor.yearly_usage'):
        yearly_usage = self.collector.get_yearly_usage_value()
        data = {
            "state": yearly_usage,
            "attributes": {
                "unit_of_measurement": "kWh",
                "friendly_name": "Yearly Energy Usage",
                "device_class": "energy",
                "state_class": "measurement"
            }
        }
        self._post_to_home_assistant(entity_id, data)

    def upload_half_hourly_usage(self, entity_id='sensor.half_hourly_usage'):
        half_hourly_usage = self.collector.get_half_hourly_usage_value()
        data = {
            "state": half_hourly_usage,
            "attributes": {
                "unit_of_measurement": "kWh",
                "friendly_name": "Half-Hourly Energy Usage",
                "device_class": "energy",
                "state_class": "measurement"
            }
        }
        self._post_to_home_assistant(entity_id, data)

    def upload_remaining_balance(self, entity_id='sensor.remaining_balance'):
        remaining_balance = self.collector.get_remaining_balance()
        data = {
            "state": remaining_balance,
            "attributes": {
                "unit_of_measurement": "CNY",
                "friendly_name": "Remaining Balance",
                "device_class": "monetary"
            }
        }
        self._post_to_home_assistant(entity_id, data)

    def upload_remaining_charge(self, entity_id='sensor.remaining_balance'):
        remaining_balance = self.collector.get_remaining_balance()
        data = {
            "state": remaining_balance,
            "attributes": {
                "unit_of_measurement": "kWh",
                "friendly_name": "Remaining Charge",
                "device_class": "energy",
                "state_class": "measurement"
            }
        }
        self._post_to_home_assistant(entity_id, data)

    def _post_to_home_assistant(self, entity_id, data):
        url = f"{self.hass_url}/api/states/{entity_id}"
        response = requests.post(url, headers=self.headers, json=data)
        if response.status_code == 200:
            logging.info(f"成功上传到 {entity_id}: {data['state']} {data['attributes']['unit_of_measurement']}")
        else:
            logging.error(f"上传到 {entity_id} 失败: {response.text}")