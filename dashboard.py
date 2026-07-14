import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time

st.set_page_config(page_title="Banking Monitor", layout="wide")
st.title("📈 Real-Time User Feedback & Sentiment Monitoring Platform")

client = MongoClient('mongodb://localhost:27017/')
db = client['banking_metrics']
coll = db['feedback_data']

placeholder = st.empty()

while True:
    # ดึงข้อมูลล่าสุด 100 รายการมาแปลงเป็น Dataframe
    cursor = coll.find().sort("_id", -1).limit(100)
    data = list(cursor)
    
    if data:
        df = pd.DataFrame(data)
        
        # 📐 สูตรคณิตศาสตร์คำนวณสัญญาณแจ้งเตือนวิกฤต (Threshold)
        df['MA_Short'] = df['negative_score'].rolling(window=5).mean()
        df['MA_Long'] = df['negative_score'].rolling(window=20).mean()
        
        recent_neg = df.head(10)['sentiment'].value_counts().get('Negative', 0)
        previous_neg = df.iloc[10:20]['sentiment'].value_counts().get('Negative', 0) if len(df) > 20 else 0
        rate_of_change = recent_neg - previous_neg
        
        with placeholder.container():
            # ดักจับจุดวิกฤตจากอัตราเร่งข้อมูลความชัน
            if rate_of_change >= 3:
                st.error(f"🚨 CRITICAL ALERT: System Disturbance Detected! (Rate of Change: +{rate_of_change})")
            else:
                st.success("🟢 System Status: Operating Within Normal Parameters")
                
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Sentiment Ratio")
                st.write(df['sentiment'].value_counts())
            with c2:
                st.subheader("Issue Tracing")
                st.write(df['topic'].value_counts())
                
            st.subheader("Mathematical Signal Analysis (Moving Average)")
            st.line_chart(df[['MA_Short', 'MA_Long']])
            
    time.sleep(1.5)