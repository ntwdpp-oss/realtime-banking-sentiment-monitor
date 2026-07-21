import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import time

st.set_page_config(page_title="Banking System Feedback Dashboard", layout="wide")
st.title("📈 Real-Time Banking Feedback & Health Monitor")

client = MongoClient('mongodb://localhost:27017/')
db = client['banking_metrics']
coll = db['feedback_data']
stats_coll = db['topic_stats']

st.sidebar.header("⚙️ Dashboard Controls")
view_mode = st.sidebar.selectbox(
    "เลือกช่วงเวลาการแสดงผล (Timeframe):",
    [
        "⚡ Real-time (Live Streaming)",
        "📅 ย้อนหลัง 1 วัน (Past 24 Hours)",
        "📅 ย้อนหลัง 3 วัน (Past 3 Days)",
        "📅 ย้อนหลัง 7 วัน (Past 7 Days)"
    ]
)

placeholder = st.empty()

while True:
    now_utc = datetime.now(timezone.utc)
    
    # 1. 📊 ดึงข้อมูลสถิติมวลรวมจากตารางแยก (topic_stats)
    overall_stats = list(stats_coll.find({}, {"_id": 0, "topic": 1, "total_count": 1}))
    
    # 2. 🔍 กรองข้อมูลตามช่วงเวลาที่ผู้ใช้เลือกบน Sidebar
    if "Real-time" in view_mode:
        cursor = coll.find().sort("_id", -1).limit(100)
    elif "1 วัน" in view_mode:
        start_time = now_utc - timedelta(days=1)
        cursor = coll.find({"created_at": {"$gte": start_time}}).sort("_id", -1)
    elif "3 วัน" in view_mode:
        start_time = now_utc - timedelta(days=3)
        cursor = coll.find({"created_at": {"$gte": start_time}}).sort("_id", -1)
    else:  # 7 วัน
        start_time = now_utc - timedelta(days=7)
        cursor = coll.find({"created_at": {"$gte": start_time}}).sort("_id", -1)

    data = list(cursor)
    
    with placeholder.container():
        # SECTION A: ภาพรวมภาพรวมสถิติทั้งหมด (Overall App Health Stats)
        st.subheader("📊 สถิติมวลรวมแอปพลิเคชัน (Overall Topic Summary)")
        if overall_stats:
            stats_df = pd.DataFrame(overall_stats).sort_values(by="total_count", ascending=False)
            
            # แสดงสถิติเป็นการ์ดตัวเลข (KPI Cards)
            cols = st.columns(len(stats_df) if len(stats_df) <= 5 else 5)
            for idx, row in enumerate(stats_df.iloc[:5].itertuples()):
                with cols[idx % 5]:
                    st.metric(label=row.topic, value=f"{row.total_count} ข้อความ")
        else:
            st.info("ยังไม่มีข้อมูลสถิติมวลรวมในระบบ")

        st.divider()

        # SECTION B: ข้อมูลการวิเคราะห์ตามช่วงเวลาที่เลือก
        st.subheader(f"📌 ผลการวิเคราะห์ช่วงเวลา: {view_mode}")
        
        if data:
            df = pd.DataFrame(data)
            df['MA_Short'] = df['negative_score'].rolling(window=5).mean()
            df['MA_Long'] = df['negative_score'].rolling(window=20).mean()
            
            # การดักจับ Alert ในกรอบเวลาปัจจุบัน
            recent_window = df.head(15)
            negative_count = recent_window[recent_window['sentiment'] == 'Negative'].shape[0]
            
            # 🚨 Alert Banner 3 ระดับ
            if negative_count > 5:
                st.error(f"🔴 CRITICAL ALERT: พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ ในช่วงเวลานี้!")
            elif 3 <= negative_count <= 5:
                st.warning(f"🟡 CAUTION WARNING: พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ ควรรีบตรวจสอบ")
            else:
                st.success(f"🟢 System Status: Normal (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ)")
            
            # 📊 🌟 จัดเลย์เอาต์ใหม่ แบ่งเป็น 3 แคตตากอรีหลัก (ดี / ทั่วไป / ปัญหาต้องแก้)
            col_good, col_neutral, col_bad = st.columns(3)
            
            with col_good:
                st.markdown("### 🟢 ฝั่งชื่นชม (Positive)")
                df_good = df[df['sentiment'] == 'Positive']
                st.metric("รวมข้อความชื่นชม", f"{len(df_good)} รายการ")
                if not df_good.empty:
                    good_counts = df_good['topic'].value_counts()
                    st.bar_chart(good_counts, color="#2ecc71")
                else:
                    st.info("ยังไม่มีข้อมูลชื่นชมในช่วงเวลานี้")
                    
            with col_neutral:
                st.markdown("### ⚪ ฝั่งสอบถาม / ทั่วไป (Neutral)")
                df_neutral = df[df['sentiment'] == 'Neutral']
                st.metric("รวมข้อความสอบถาม", f"{len(df_neutral)} รายการ")
                if not df_neutral.empty:
                    neutral_counts = df_neutral['topic'].value_counts()
                    st.bar_chart(neutral_counts, color="#3498db")
                else:
                    st.info("ยังไม่มีข้อความทั่วไป/สอบถาม")

            with col_bad:
                st.markdown("### 🔴 ฝั่งปัญหาต้องแก้ไข (Negative)")
                df_bad = df[df['sentiment'] == 'Negative']
                st.metric("รวมปัญหาที่พบ", f"{len(df_bad)} รายการ")
                if not df_bad.empty:
                    bad_counts = df_bad['topic'].value_counts()
                    st.bar_chart(bad_counts, color="#e74c3c")
                else:
                    st.info("ไม่มีข้อมูลปัญหาในช่วงเวลานี้")

            # 📈 กราฟสัญญาณคณิตศาสตร์
            st.markdown("### 📉 Signal Trend Analysis (Moving Average)")
            st.line_chart(df[['MA_Short', 'MA_Long']])
        else:
            st.warning("⚠️ ไม่พบข้อมูลในช่วงเวลาที่เลือก")
            
    # หน่วงเวลา 2 วินาทีหากอยู่ในโหมด Real-time
    time.sleep(2.0)