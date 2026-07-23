import json
import time
import websocket
from kafka import KafkaProducer

KAFKA_TOPIC = "crypto_trades"
KAFKA_SERVER = "localhost:9092"

def get_kafka_producer():
    # Wait for Kafka container startup
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka Broker successfully.")
            return producer
        except Exception as e:
            print("Waiting for Kafka broker to be available...")
            time.sleep(3)

producer = get_kafka_producer()

def on_message(ws, message):
    data = json.loads(message)
    
    # Extract & format clean trade payload
    trade = {
        "symbol": data["s"],
        "timestamp": int(data["T"]), # Milliseconds epoch
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "trade_id": int(data["t"])
    }
    
    producer.send(KAFKA_TOPIC, value=trade)
    print(f"[KAFKA PRODUCER] Published: {trade['symbol']} | Price: ${trade['price']} | Vol: {trade['quantity']}")

def on_error(ws, error):
    print("WebSocket Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket Closed.")

def on_open(ws):
    print("Connected to Binance WebSocket Stream. Receiving trade ticks...")

if __name__ == "__main__":
    # Binance multi-stream URL for BTC and ETH
    socket_url = "wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()