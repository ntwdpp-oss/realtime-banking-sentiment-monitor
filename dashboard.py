import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time

st.set_page_config(page_title="Banking Monitor", layout="wide")
st.title("📈 Real-Time User Feedback & Sentiment Monitoring Platform")

# เชื่อมต่อ MongoDB (Port 27017 / DB: banking_metrics / Collection: feedback_data)
try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
    db = client['banking_metrics']
    coll = db['feedback_data']
    client.server_info()
except Exception as e:
    st.error(f"❌ ไม่สามารถเชื่อมต่อ MongoDB ได้: {e}")
    st.stop()

placeholder = st.empty()

while True:
    # ดึงข้อมูลล่าสุด 100 รายการ
    cursor = coll.find().sort("_id", -1).limit(100)
    data = list(cursor)
    
    if data:
        # แปลงเป็น DataFrame และเรียงลำดับเวลาจากอดีตไปปัจจุบัน เพื่อคำนวณ Moving Average ได้ถูกต้อง
        df = pd.DataFrame(data)
        df = df.sort_values("_id", ascending=True).reset_index(drop=True)
        
        # คำนวณ Moving Average ของ negative_score ที่พ่นมาจาก AI
        if 'negative_score' in df.columns:
            df['MA_Short'] = df['negative_score'].rolling(window=5, min_periods=1).mean()
            df['MA_Long'] = df['negative_score'].rolling(window=20, min_periods=1).mean()
        else:
            df['MA_Short'] = 0
            df['MA_Long'] = 0

        # นับจำนวนคอมเมนต์เชิงลบใน 15 รายการล่าสุด
        recent_window = df.tail(15)
        negative_count = recent_window[recent_window['sentiment'] == 'Negative'].shape[0] if 'sentiment' in recent_window.columns else 0
        
        with placeholder.container():
            
            # 🚨 1. ระบบแจ้งเตือน 3 ระดับ (ตามจำนวนข้อความเชิงลบ)
            if negative_count > 5:
                st.error(f"🔴 CRITICAL ALERT: System Failure Likely! (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ ในช่วงเวลาล่าสุด)")
            elif 3 <= negative_count <= 5:
                st.warning(f"🟡 CAUTION WARNING: Unusual Activity. Please Monitor. (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ)")
            else:
                st.success(f"🟢 System Status: Healthy (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ)")
                
            # 📊 2. แยกกราฟดี (ซ้าย) - แย่ (ขวา) แยกตาม 6 หมวดหมู่ Topic
            col_good, col_bad = st.columns(2)
            
            with col_good:
                st.markdown("### 🟢 ฝั่งชื่นชมของผู้ใช้งาน (Positive Feedback)")
                df_good = df[df['sentiment'] == 'Positive']
                if not df_good.empty and 'topic' in df_good.columns:
                    good_counts = df_good['topic'].value_counts()
                    st.bar_chart(good_counts, color="#2ecc71")
                else:
                    st.info("ยังไม่มีข้อมูลชื่นชมในระบบ")
                    
            with col_bad:
                st.markdown("### 🔴 ฝั่งปัญหาที่ต้องแก้ไข (Negative Issues)")
                df_bad = df[df['sentiment'] == 'Negative']
                if not df_bad.empty and 'topic' in df_bad.columns:
                    bad_counts = df_bad['topic'].value_counts()
                    st.bar_chart(bad_counts, color="#e74c3c")
                else:
                    st.info("ระบบปกติ ดีเยี่ยม ยังไม่มีเสียงบ่น")

            # 📈 3. กราฟเส้น MA แสดงสัญญาณการสะสมตัวของ Sentiment เชิงลบ
            st.subheader("Mathematical Signal Analysis (Moving Average)")
            if 'MA_Short' in df.columns and 'MA_Long' in df.columns:
                st.line_chart(df[['MA_Short', 'MA_Long']])
            
            

    else:
        with placeholder.container():
            st.info("⏳ กำลังรอข้อมูลจาก Kafka / AI Processor...")

    time.sleep(1.5)