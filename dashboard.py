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
    # ดึงข้อมูลล่าสุด 100 รายการ
    cursor = coll.find().sort("_id", -1).limit(100)
    data = list(cursor)
    
    if data:
        df = pd.DataFrame(data)
        
        # คำนวณ Moving Average ไว้โชว์กราฟเส้นเหมือนเดิม
        df['MA_Short'] = df['negative_score'].rolling(window=5).mean()
        df['MA_Long'] = df['negative_score'].rolling(window=20).mean()
        
        # 🌟 จุดที่ปรับแก้: นับจำนวนคอมเมนต์เชิงลบใน 15 รายการล่าสุด (จำลองช่วงเวลาหน้าต่าง)
        recent_window = df.head(15)
        negative_count = recent_window[recent_window['sentiment'] == 'Negative'].shape[0]
        
        with placeholder.container():
            
            # 🚨 1. ระบบแจ้งเตือน 3 ระดับ (ตามจำนวนข้อความเชิงลบ)
            if negative_count > 5:
                st.error(f"🔴 CRITICAL ALERT: System Failure Likely! (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ ในช่วงเวลาล่าสุด)")
            elif 3 <= negative_count <= 5:
                st.warning(f"🟡 CAUTION WARNING: Unusual Activity. Please Monitor. (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ)")
            else:
                st.success(f"🟢 System Status: Healthy (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ)")
                
            # 📊 2. แยกกราฟดี (ซ้าย) - แย่ (ขวา) แบบที่คุณต้องการ
            col_good, col_bad = st.columns(2)
            
            with col_good:
                st.markdown("### 🟢 ฝั่งชื่นชมของผู้ใช้งาน (Positive Feedback)")
                df_good = df[df['sentiment'] == 'Positive']
                if not df_good.empty:
                    good_counts = df_good['topic'].value_counts()
                    st.bar_chart(good_counts, color="#2ecc71")
                else:
                    st.info("ยังไม่มีข้อมูลชื่นชมในระบบ")
                    
            with col_bad:
                st.markdown("### 🔴 ฝั่งปัญหาที่ต้องแก้ไข (Negative Issues)")
                df_bad = df[df['sentiment'] == 'Negative']
                if not df_bad.empty:
                    bad_counts = df_bad['topic'].value_counts()
                    st.bar_chart(bad_counts, color="#e74c3c")
                else:
                    st.info("ระบบปกติ ดีเยี่ยม ยังไม่มีเสียงบ่น")

            # 📈 3. กราฟเส้น MA ยังอยู่เหมือนเดิมให้อาจารย์ดู
            st.subheader("Mathematical Signal Analysis (Moving Average)")
            st.line_chart(df[['MA_Short', 'MA_Long']])
            
    time.sleep(1.5)