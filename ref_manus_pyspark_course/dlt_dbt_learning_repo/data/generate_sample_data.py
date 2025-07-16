import pandas as pd
import numpy as np
import os

def generate_sales_data(num_records=1000, output_dir="../data/raw"):
    """
    Generates synthetic sales data and saves it as a CSV file.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Webcam", "Headphones"]
    customers = [f"Customer_{i}" for i in range(1, 51)]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]

    data = {
        "transaction_id": np.arange(1, num_records + 1),
        "product_name": np.random.choice(products, num_records),
        "customer_name": np.random.choice(customers, num_records),
        "city": np.random.choice(cities, num_records),
        "quantity": np.random.randint(1, 5, num_records),
        "price": np.round(np.random.uniform(10.0, 1000.0, num_records), 2),
        "transaction_date": pd.to_datetime(np.random.choice(pd.date_range('2024-01-01', '2024-12-31'), num_records)).strftime('%Y-%m-%d')
    }

    df = pd.DataFrame(data)
    output_path = os.path.join(output_dir, "sales_data.csv")
    df.to_csv(output_path, index=False)
    print(f"Generated {num_records} records and saved to {output_path}")

if __name__ == "__main__":
    generate_sales_data()


