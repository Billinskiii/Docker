from faker import Faker
import psycopg2
import uuid
import random
import time
from datetime import datetime

# Inisialisasi Faker
fake = Faker()

# Koneksi ke PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    database="ecommerce",  # Nama database Anda
    user="user",           # User PostgreSQL Anda
    password="password"    # Password PostgreSQL Anda
)
cursor = connection.cursor()

# Daftar produk dan metode pembayaran yang disesuaikan
products = {
    'Laptop': 1500,
    'Smartphone': 950,
    'Tablet': 1250,
    'Headphones': 450,
    'Camera': 1400
}
payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash On Delivery']

# Query SQL untuk memasukkan data ke tabel transactions
insert_query = """
    INSERT INTO public.transactions (transaction_id, customer_name, product, quantity, price, total_price, payment_method, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Fungsi untuk menghasilkan data transaksi
def create_transaction():
    transaction_id = str(uuid.uuid4())  # UUID untuk transaction_id
    customer_name = fake.name()  # Nama pelanggan
    product = random.choice(list(products.keys()))  # Pilih produk dari daftar
    price = products[product]  # Ambil harga produk sesuai dengan daftar
    quantity = random.randint(1, 5)  # Kuantitas antara 1 dan 5
    total_price = round(quantity * price, 2)  # Total harga
    payment_method = random.choice(payment_methods)  # Pilih metode pembayaran dari daftar
    timestamp = fake.date_time_this_year()  # Timestamp acak dari tahun ini

    return (
        transaction_id,
        customer_name,
        product,
        quantity,
        price,
        total_price,
        payment_method,
        timestamp
    )

# Fungsi untuk memasukkan data ke PostgreSQL
def insert_transaction():
    transaction = create_transaction()
    try:
        cursor.execute(insert_query, transaction)
        connection.commit()  # Commit perubahan ke database
        print("Transaction inserted:", transaction)
    except Exception as e:
        print(f"Error inserting transaction: {e}")
        connection.rollback()  # Rollback jika terjadi kesalahan

# Menghasilkan data transaksi setiap 30 detik
try:
    while True:
        insert_transaction()
        time.sleep(30)  # Tunggu selama 30 detik sebelum menghasilkan transaksi baru
except KeyboardInterrupt:
    print("Real-time data generation stopped.")

# Tutup koneksi
cursor.close()
connection.close()
