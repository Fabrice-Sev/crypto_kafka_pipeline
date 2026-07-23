import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

st.set_page_config(page_title="Real-Time Crypto Stream", layout="wide")
st.title("⚡ Real-Time Crypto Analytics (Kafka + Spark + MongoDB)")

placeholder = st.empty()

while True:
    # Query latest 200 live trade documents from MongoDB
    cursor = collection.find().sort("timestamp", -1).limit(200)
    data = list(cursor)

    if data:
        df = pd.DataFrame(data)

        with placeholder.container():
            col1, col2 = st.columns(2)

            # Get latest trade prices
            btc_df = df[df['symbol'] == 'BTCUSDT']
            eth_df = df[df['symbol'] == 'ETHUSDT']

            btc_price = btc_df['price'].iloc[0] if not btc_df.empty else 0
            eth_price = eth_df['price'].iloc[0] if not eth_df.empty else 0

            col1.metric("Live BTC/USDT", f"${btc_price:,.2f}")
            col2.metric("Live ETH/USDT", f"${eth_price:,.2f}")

            # Plot dynamic line chart
            fig = px.line(
                df, 
                x="timestamp", 
                y="price", 
                color="symbol", 
                title="Real-Time Market Price Movement",
                markers=True
            )
            st.plotly_chart(
                fig, 
                use_container_width=True,
                key=f"crypto_chart_{int(time.time())}"
            )

            # Raw tabular view
            st.subheader("Raw MongoDB Documents")
            st.dataframe(df[['symbol', 'timestamp', 'price', 'quantity', 'trade_id']].head(10))

    time.sleep(1) # Refresh UI interval