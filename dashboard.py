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
        
        # with placeholder.container():
        #     # ดักจับจุดวิกฤตจากอัตราเร่งข้อมูลความชัน
        #     if rate_of_change >= 3:
        #         st.error(f"🚨 CRITICAL ALERT: System Disturbance Detected! (Rate of Change: +{rate_of_change})")
        #     else:
        #         st.success("🟢 System Status: Operating Within Normal Parameters")
                
        #     c1, c2 = st.columns(2)
        #     with c1:
        #         st.subheader("Sentiment Ratio")
        #         st.write(df['sentiment'].value_counts())
        #     with c2:
        #         st.subheader("Issue Tracing")
        #         st.write(df['topic'].value_counts())
                
        #     st.subheader("Mathematical Signal Analysis (Moving Average)")
        #     st.line_chart(df[['MA_Short', 'MA_Long']])
        with placeholder.container():
            # (โค้ดส่วนคำนวณ Threshold และ Alert คงไว้เหมือนเดิม)
            if rate_of_change >= 3:
                st.error(f"🚨 CRITICAL ALERT: System Disturbance Detected! (Rate of Change: +{rate_of_change})")
            else:
                st.success("🟢 System Status: Operating Within Normal Parameters")
            
            # ----------------------------------------------------
            # 🌟 จุดปรับปรุงใหม่: แยกมิติกราฟดี-แย่ ออกจากกันชัดเจน
            # ----------------------------------------------------
            
            # แบ่งเป็น 2 คอลัมน์ใหญ่ ซ้าย (ฝั่งชม) - ขวา (ฝั่งบ่น)
            col_good, col_bad = st.columns(2)
            
            with col_good:
                st.markdown("### 🟢 ฝั่งชื่นชมของผู้ใช้งาน (Positive Feedback)")
                # ดักกรองเอาเฉพาะข้อมูลที่เป็น Positive มานับแยกประเภท
                df_good = df[df['sentiment'] == 'Positive']
                if not df_good.empty:
                    good_counts = df_good['topic'].value_counts()
                    st.bar_chart(good_counts, color="#2ecc71") # กราฟแท่งสีเขียว
                else:
                    st.info("ยังไม่มีข้อมูลชื่นชมในระบบ")
                    
            with col_bad:
                st.markdown("### 🔴 ฝั่งปัญหาที่ต้องแก้ไข (Negative Issues)")
                # ดักกรองเอาเฉพาะข้อมูลที่เป็น Negative มานับแยกประเภท
                df_bad = df[df['sentiment'] == 'Negative']
                if not df_bad.empty:
                    bad_counts = df_bad['topic'].value_counts()
                    st.bar_chart(bad_counts, color="#e74c3c") # กราฟแท่งสีแดง
                else:
                    st.info("ระบบปกติ ดีเยี่ยม ยังไม่มีเสียงบ่น")

            # แสดงกราฟเส้นสัญญาณคณิตศาสตร์ไว้ด้านล่างสุดเหมือนเดิม
            st.subheader("Mathematical Signal Analysis (Moving Average)")
            st.line_chart(df[['MA_Short', 'MA_Long']])
            
    time.sleep(1.5)