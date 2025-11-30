import csv
import random
from datetime import datetime, timedelta, timezone
import uuid

def generate_test_csv(filename, num_records=100000):
    """Generate test CSV data for the analytics engine"""
    
    zones = ['North', 'South', 'East', 'West', 'Central', 'Downtown', 'Suburb']
    categories = ['Food', 'Electronics', 'Clothing', 'Books', 'Health', 'Sports', 'Home']
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['TRANSACTION_ID', 'MERCHANT_ID', 'ZONE', 'CATEGORY', 'AMOUNT', 'TIMESTAMP', 'CUSTOMER_PHONE']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        base_date = datetime.now(timezone.utc) - timedelta(days=30)
        
        for i in range(num_records):
            # Generate realistic data
            transaction_id = f"TXN_{uuid.uuid4().hex[:8].upper()}"
            merchant_id = f"MERCH_{random.randint(1000, 9999)}"
            zone = random.choice(zones)
            category = random.choice(categories)
            
            # Amount with some outliers for anomaly detection
            if random.random() < 0.001:  # 0.1% outliers
                amount = round(random.uniform(10000, 50000), 2)
            else:
                amount = round(random.uniform(10, 1000), 2)
            
            # Random timestamp within last 30 days
            timestamp = base_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            # Customer phone with some repeat customers
            if random.random() < 0.3:  # 30% repeat customers
                customer_phone = f"+1555{random.randint(1000000, 1000100):07d}"
            else:
                customer_phone = f"+1555{random.randint(1000000, 9999999):07d}"
            
            writer.writerow({
                'TRANSACTION_ID': transaction_id,
                'MERCHANT_ID': merchant_id,
                'ZONE': zone,
                'CATEGORY': category,
                'AMOUNT': amount,
                'TIMESTAMP': timestamp.isoformat(),
                'CUSTOMER_PHONE': customer_phone
            })
            
            if i % 10000 == 0:
                print(f"Generated {i} records...")
    
    print(f"Test data generated: {filename} with {num_records} records")

if __name__ == "__main__":
    # Generate different sized test files
    generate_test_csv("../data/test_small.csv", 1000)
    generate_test_csv("../data/test_medium.csv", 100000)
    generate_test_csv("../data/test_large.csv", 1000000)