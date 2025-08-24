import requests
def send_alert(message):
  token = 'YOUR_TELEGRAM_TOKEN'
  chat_id = 'YOUR_CHAT_ID'
  url = f'https://api.telegram.org/bot{token}/sendMessage'
  requests.post(url, data={'chat_id': chat_id, 'text': message})