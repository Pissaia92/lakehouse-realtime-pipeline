import random
import pandas as pd
from datetime import datetime, timedelta

def generate_data():
    data = []
    status_list = ["pending", "shipped", "delivered"]
    region_list = ["North", "South", "East", "West"]
    payment_list = ["Credit Card", "PayPal", "Bank Transfer"]

    start_date = datetime(2025, 8, 24, 15, 0, 0)
    for i in range(1000):
        order_id = 1000 + i
        customer_id = random.randint(1, 100)
        amount = round(random.uniform(10, 500), 2)
        created_at = start_date + timedelta(seconds=i)
        status = random.choice(status_list)
        region = random.choice(region_list)
        payment_method = random.choice(payment_list)
        data.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": amount,
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "region": region,
            "payment_method": payment_method
        })
    return data

df = pd.DataFrame(generate_data())
df.to_csv("sample_orders.csv", index=False)
print("✅ 1000 orders generated and saved to sample_orders.csv")