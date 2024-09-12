import time
import json
from faker import Faker
from kafka import KafkaProducer

# Inisialisasi Faker dan Kafka producer
fake = Faker()
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Fungsi untuk menghasilkan data transaksi dummy
def generate_dummy_transaction():
    return {
        'Transaction ID': fake.uuid4(),
        'Customer ID': fake.uuid4(),
        'Product ID': fake.ean8(),
        'Category': fake.word(),
        'Price': round(fake.random_number(digits=2), 2),
        'Quantity': fake.random_int(min=1, max=5),
        'Total Amount': round(fake.random_number(digits=2) * fake.random_int(min=1, max=5), 2),
        'Transaction Date': fake.date_time_this_year().isoformat(),
        'Payment Method': fake.credit_card_provider(),
        'Shipping Address': fake.address(),
        'Customer Region': fake.state()
    }

# Loop untuk menghasilkan data setiap 30 detik
while True:
    transaction = generate_dummy_transaction()
    print(f"Sending data: {transaction}")
    producer.send('ecommerce-transactions', transaction)  # Mengirim data ke Kafka topic
    time.sleep(30)  # Tunggu 30 detik sebelum mengirim data baru
