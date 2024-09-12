from confluent_kafka import Consumer, KafkaException
import psycopg2
import json

# Koneksi ke PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    database="ecommerce",  # Ganti dengan nama database Anda
    user="user",           # Ganti dengan user PostgreSQL Anda
    password="password"    # Ganti dengan password PostgreSQL Anda
)
cursor = connection.cursor()

# Konfigurasi Kafka Consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'group.id': 'ecommerce_group',          # Group ID untuk Kafka Consumer
    'auto.offset.reset': 'earliest'         # Mulai dari offset paling awal jika tidak ada commit offset
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['transactions'])  # Nama topik Kafka

# Query SQL untuk memasukkan data ke PostgreSQL
insert_query = """
    INSERT INTO transactions (transaction_id, customer_name, product, quantity, price, total_price, payment_method, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Menunggu pesan dari Kafka
        if msg is None:
            continue  # Jika tidak ada pesan, lanjutkan polling
        if msg.error():
            raise KafkaException(msg.error())  # Jika ada error di Kafka, tampilkan exception

        # Deserialize pesan Kafka yang diterima (format JSON)
        transaction = json.loads(msg.value().decode('utf-8'))
        print(f"Received transaction: {transaction}")

        # Siapkan data untuk dimasukkan ke PostgreSQL
        data = (
            transaction['transaction_id'],      # UUID transaksi
            transaction['customer_name'],       # Nama pelanggan
            transaction['product'],             # Nama produk
            transaction['quantity'],            # Jumlah produk
            transaction['price'],               # Harga satuan
            transaction['total_price'],         # Total harga
            transaction['payment_method'],      # Metode pembayaran
            transaction['timestamp']            # Timestamp
        )

        # Masukkan data ke PostgreSQL
        try:
            cursor.execute(insert_query, data)
            connection.commit()  # Commit perubahan ke database
            print("Transaction saved to PostgreSQL.")
        except Exception as e:
            print(f"Error inserting transaction: {e}")
            connection.rollback()  # Rollback jika ada kesalahan

except KeyboardInterrupt:
    print("Consumer interrupted. Closing connection...")
finally:
    # Tutup Kafka Consumer dan koneksi PostgreSQL
    consumer.close()
    cursor.close()
    connection.close()
