"""
Practical Task 2: Analyzing and Visualizing E-Commerce Transactions with NumPy
"""
import numpy as np
import random
from datetime import datetime, timedelta
np.random.seed(42)


ARRAY_SIZE = 10
COLUMNS = ["transaction_id", "user_id", "product_id", "quantity", "price", "timestamp"]


def generate_timestamps(start_date: str, length: int) -> np.ndarray:
    start = datetime.strptime(start_date, "%Y%m%d")
    timestamps = [int((start + timedelta(days=i)).strftime("%Y%m%d")) for i in range(length)]
    return np.array(timestamps)

# Create a sample array
def array_creation(array_size):
    transaction_ids = np.arange(1, array_size + 1)
    user_ids = np.arange(101, 101+array_size)
    product_ids = np.random.randint(100, 200, size=array_size)
    quantities = np.random.randint(1, 10, size=array_size)
    prices = np.random.randint(10, 100, size=array_size)
    timestamps = np.array(generate_timestamps(start_date='20240101', length=array_size))
    transactions = np.column_stack((transaction_ids, user_ids, product_ids, quantities, prices, timestamps))
    return transactions

def total_revenue(transactions: np.ndarray) -> float:
    quantities = transactions[:, 3].astype(float)
    prices = transactions[:, 4].astype(float)
    revenue = np.sum(quantities * prices)
    return revenue

def unique_users(transactions: np.ndarray) -> int:
    users = transactions[:, 1]
    unique_users = np.unique(users)
    return len(unique_users)

def most_purchased_product(transactions: np.ndarray) -> int:
    products = transactions[:, 2]
    quantities = transactions[:, 3].astype(int)
    product_quantities = {}
    for product, quantity in zip(products, quantities):
        if product in product_quantities:
            product_quantities[product] += quantity
        else:
            product_quantities[product] = quantity
    most_purchased = max(product_quantities, key=product_quantities.get)
    return most_purchased

def convert_price_to_int(transactions: np.ndarray) -> np.ndarray:
    return transactions[:, 4].astype(int)

def check_data_types(transactions: np.ndarray) -> dict:
    data_types = {COLUMNS[i]: transactions[:, i].dtype for i in range(transactions.shape[1])}
    return data_types

def product_quantity_array(transactions: np.ndarray) -> np.ndarray:
    return transactions[:, [2, 3]]

def user_transaction_count(transactions: np.ndarray) -> np.ndarray:
    users, counts = np.unique(transactions[:, 1], return_counts=True)
    return np.array(list(zip(users, counts)))

def masked_array(transactions: np.ndarray) -> np.ndarray:
    mask = transactions[:, 3] != 0
    return transactions[mask]

def price_increase(transactions: np.ndarray, percentage: float) -> np.ndarray:
    return transactions[:, 4].astype(float) * (1 + percentage / 100)

def filter_transactions(transactions: np.ndarray) -> np.ndarray:
    return transactions[transactions[:, 3].astype(int) > 1]

def revenue_comparison(transactions: np.ndarray, start_date1: int, end_date1: int, start_date2: int, end_date2: int) -> dict:
    period1 = transactions[(transactions[:, 5].astype(int) >= start_date1) & (transactions[:, 5].astype(int) <= end_date1)]
    period2 = transactions[(transactions[:, 5].astype(int) >= start_date2) & (transactions[:, 5].astype(int) <= end_date2)]
    revenue1 = total_revenue(period1)
    revenue2 = total_revenue(period2)
    return {"period1": revenue1, "period2": revenue2}

def user_transactions(transactions: np.ndarray, user_id: int) -> np.ndarray:
    return transactions[transactions[:, 1].astype(int) == user_id]

def date_range_slicing(transactions: np.ndarray, start_date: int, end_date: int) -> np.ndarray:
    return transactions[(transactions[:, 5].astype(int) >= start_date) & (transactions[:, 5].astype(int) <= end_date)]

def top_products(transactions: np.ndarray, top_n: int) -> np.ndarray:
    quantities = transactions[:, 3].astype(float)
    prices = transactions[:, 4].astype(float)
    revenues = quantities * prices
    sorted_indices = np.argsort(revenues)[-top_n:]
    return transactions[sorted_indices]

def print_array(array, message=None):
    if message:
        print(message)
    print(array, end="\n\n")


if __name__ == '__main__':
    transactions = array_creation(ARRAY_SIZE)
    print_array(transactions, "Initial Transactions Array:")
    
    revenue = total_revenue(transactions)
    print_array(revenue, "Total Revenue:")
    
    num_unique_users = unique_users(transactions)
    print_array(num_unique_users, "Number of Unique Users:")
    
    most_purchased = most_purchased_product(transactions)
    print_array(most_purchased, "Most Purchased Product ID:")
    
    int_price = convert_price_to_int(transactions)
    print_array(int_price, "Transactions Array with Price Converted to Integer:")
    
    data_types = check_data_types(transactions)
    print_array(data_types, "Data Types:")
    
    prod_quantity = product_quantity_array(transactions)
    print_array(prod_quantity, "Product Quantity Array:")
    
    user_trans_count = user_transaction_count(transactions)
    print_array(user_trans_count, "User Transaction Count Array:")
    
    masked_trans = masked_array(transactions)
    print_array(masked_trans, "Masked Transactions Array (Quantity > 0):")
    
    increased_prices = price_increase(transactions, 5)
    print_array(increased_prices, "Transactions Array with 5% Price Increase:")
    
    filtered_trans = filter_transactions(transactions)
    print_array(filtered_trans, "Filtered Transactions (Quantity > 1):")
    
    revenue_comp = revenue_comparison(transactions, 20240101, 20240103, 20240108, 20240110)
    print_array(revenue_comp, "Revenue Comparison:")
    
    user_trans = user_transactions(transactions, 101)
    print_array(user_trans, "Transactions for User 101:")
    
    date_range_trans = date_range_slicing(transactions, 20240101, 20240105)
    print_array(date_range_trans, "Transactions from Date Range 20240101 to 20240105:")
    
    top_prod_trans = top_products(transactions, 5)
    print_array(top_prod_trans, "Top 5 Products by Revenue:")

    assert transactions.shape == (ARRAY_SIZE, 6), f"Shape of the transactions array should be ({ARRAY_SIZE}, 6)"
    assert unique_users(transactions) == ARRAY_SIZE, "There should be 5 unique users"
    assert check_data_types(transactions)['price'] == int, "Price column should be of type int"
