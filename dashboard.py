import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time
from datetime import datetime, timedelta

st.set_page_config(page_title="Banking Monitor", layout="wide")
st.title("📈 Real-Time User Feedback & Sentiment Monitoring Platform")

# --- 1. เชื่อมต่อ MongoDB ---
try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
    db = client['banking_metrics']
    coll = db['feedback_data']
    client.server_info()
except Exception as e:
    st.error(f"❌ ไม่สามารถเชื่อมต่อ MongoDB ได้: {e}")
    st.stop()

# --- 2. ตัวเลือกดูย้อนหลังใน Sidebar ---
st.sidebar.title("⚙️ แผงควบคุม")
mode = st.sidebar.selectbox(
    "⏱️ เลือกโหมดการแสดงผล:",
    [
        "🟢 Live Real-time (100 รายการล่าสุด)",
        "24 ชั่วโมงล่าสุด",
        "3 วันล่าสุด",
        "7 วันล่าสุด"
    ],
    index=0
)

placeholder = st.empty()

def render_dashboard(df, is_live=True):
    with placeholder.container():
        if df.empty:
            st.info("ℹ️ ไม่พบข้อมูลในช่วงเวลาที่เลือก")
            return

        # คำนวณ Moving Average
        if 'negative_score' in df.columns:
            df['MA_Short'] = df['negative_score'].rolling(window=5, min_periods=1).mean()
            df['MA_Long'] = df['negative_score'].rolling(window=20, min_periods=1).mean()
        else:
            df['MA_Short'] = 0
            df['MA_Long'] = 0

        # นับจำนวนคอมเมนต์เชิงลบ (15 รายการล่าสุด)
        recent_window = df.tail(15)
        negative_count = recent_window[recent_window['sentiment'] == 'Negative'].shape[0] if 'sentiment' in recent_window.columns else 0
        
        # 🚨 1. ระบบแจ้งเตือน 3 ระดับ
        if negative_count > 5:
            st.error(f"🔴 CRITICAL ALERT: System Failure Likely! (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ ในช่วงเวลาล่าสุด)")
        elif 3 <= negative_count <= 5:
            st.warning(f"🟡 CAUTION WARNING: Unusual Activity. Please Monitor. (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ)")
        else:
            st.success(f"🟢 System Status: Healthy (พบคำวิจารณ์เชิงลบ {negative_count} ข้อความ)")
            
        # 📊 2. แยกกราฟดี (ซ้าย) - แย่ (ขวา) แยกตาม Topic
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

        st.markdown("---")

        # 📋 4. ตารางแสดงรายการ Feedback (Data Table)
        st.subheader("📋 ตารางรายการความคิดเห็น (Feedback Data)")
        
        # เตรียมคอลัมน์ที่จะแสดง
        table_cols = [c for c in ['timestamp', 'text', 'sentiment', 'topic', 'negative_score'] if c in df.columns]
        if not table_cols:
            table_cols = df.columns.tolist()

        # แสดงรายการจากใหม่สุดไปเก่าสุด
        st.dataframe(
            df[table_cols].sort_index(ascending=False),
            use_container_width=True,
            hide_index=True
        )

# --- 3. การทำงานแยกตามโหมด ---
if mode == "🟢 Live Real-time (100 รายการล่าสุด)":
    while True:
        cursor = coll.find().sort("_id", -1).limit(100)
        data = list(cursor)
        if data:
            df = pd.DataFrame(data)
            df = df.sort_values("_id", ascending=True).reset_index(drop=True)
            render_dashboard(df, is_live=True)
        else:
            with placeholder.container():
                st.info("⏳ กำลังรอข้อมูลจาก Kafka / AI Processor...")
        time.sleep(1.5)
else:
    # โหมดย้อนหลัง (Query ตาม timestamp หรือดึงข้อมูลทั้งหมดมา filter)
    days_map = {"24 ชั่วโมงล่าสุด": 1, "3 วันล่าสุด": 3, "7 วันล่าสุด": 7}
    cutoff_time = datetime.now() - timedelta(days=days_map[mode])
    
    # Query ข้อมูลย้อนหลัง
    cursor = coll.find({"timestamp": {"$gte": cutoff_time}}).sort("_id", 1)
    data = list(cursor)
    
    # Fallback ถ้า timestamp เป็น String หรือไม่ได้เก็บเป็น datetime ให้ดึงข้อมูลทั้งหมดมาแปลง
    if not data:
        data = list(coll.find().sort("_id", 1))
    
    df = pd.DataFrame(data)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df[df['timestamp'] >= cutoff_time]
        
    render_dashboard(df, is_live=False)