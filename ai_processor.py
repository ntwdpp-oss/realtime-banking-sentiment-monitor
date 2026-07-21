import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING

print("🔌 Connecting to MongoDB & Kafka...")
db_client = MongoClient('mongodb://localhost:27017/')
db = db_client['banking_metrics']
collection = db['feedback_data']
stats_collection = db['topic_stats']

# 1. 🟢 ตั้งค่า TTL Index ลบข้อมูลอัตโนมัติเมื่อครบ 7 วัน (604,800 วินาที)
# MongoDB จะทำการตรวจสอบและลบ Document ที่มี 'created_at' เกิน 7 วันให้อัตโนมัติ
collection.create_index([("created_at", ASCENDING)], expireAfterSeconds=604800)
print("🧹 TTL Index configured: Data older than 7 days will be auto-purged by MongoDB.")

consumer = KafkaConsumer(
    'banking_feedback',
    bootstrap_servers=['127.0.0.1:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

use_transformer = False
try:
    from transformers import pipeline
    print("⏳ Loading WangchanBERTa Sentiment Model (poom-sci/WangchanBERTa-finetuned-sentiment)...")
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="poom-sci/WangchanBERTa-finetuned-sentiment"
    )
    use_transformer = True
    print("🤖 WangchanBERTa AI Model Ready!")
except Exception as e:
    print(f"⚠️ HuggingFace Model load bypassed ({e}) -> Switched to Fast Keyword Fallback Engine.")

print("🚀 AI Processor Pipeline Running...\n")

for message in consumer:
    data = message.value
    text = data['text']
    
    # 2. 🧠 ประมวลผล AI Sentiment
    if use_transformer:
        try:
            results = sentiment_pipeline(text)
            label = str(results[0]['label']).lower()
            score = float(results[0]['score'])
            
            if 'neg' in label:
                sentiment_status = "Negative"
                neg_score = round(score, 2)
            elif 'pos' in label:
                sentiment_status = "Positive"
                neg_score = round(1.0 - score, 2)
            else:
                sentiment_status = "Neutral"
                neg_score = 0.50
        except Exception:
            use_transformer = False

    # Fallback Rule-based Analysis
    if not use_transformer:
        if any(k in text for k in ["ล่ม", "หมุน", "ค้าง", "เด้ง", "บั๊ก", "ช้า", "โอนไม่ไป"]):
            sentiment_status = "Negative"
            neg_score = 0.90
        elif any(k in text for k in ["ไว", "เร็ว", "สะดวก", "สวย", "ใช้ง่าย", "ชอบ", "ดี"]):
            sentiment_status = "Positive"
            neg_score = 0.05
        else:
            sentiment_status = "Neutral"
            neg_score = 0.40

    # 🛡️ 🌟 OVERRIDE RULE: ดักจับคำประชด (ถ้าเจอคำว่าระบบพัง บังคับเป็น Negative ทันที)
    critical_error_keywords = ["ล่ม", "โอนไม่ไป", "เด้ง", "ค้าง", "ขัดข้อง", "ห่วย"]
    if any(k in text for k in critical_error_keywords):
        sentiment_status = "Negative"
        neg_score = max(neg_score, 0.85)

    # 3. 🏷️ คัดแยกหมวดหมู่ปัญหา (Topic Classification)
    if "ล่ม" in text or "โอนไม่ไป" in text:
        topic = "System Down (ระบบล่ม)"
    elif "เด้ง" in text or "ค้าง" in text:
        topic = "App Crash & Bugs (แอปค้าง)"
    elif "ช้า" in text:
        topic = "UI/UX Issues (ระบบช้า)"
    elif "ไว" in text or "เร็ว" in text or "สะดวก" in text:
        topic = "Fast Performance (บริการไว)"
    elif "สวย" in text or "ใช้ง่าย" in text or "ชอบ" in text:
        topic = "Good Experience (ชอบ/ใช้ง่าย)"
    else:
        topic = "General Inquiry (ทั่วไป)"

    current_utc = datetime.now(timezone.utc)
    data['sentiment'] = sentiment_status
    data['negative_score'] = neg_score
    data['topic'] = topic
    data['created_at'] = current_utc  # BSON Datetime สำหรับใช้ลบข้อมูลอัตโนมัติครบ 7 วัน

    # 4. 🗄️ บันทึกคอมเมนต์ลงตารางหลัก (feedback_data)
    collection.insert_one(data)

    # 5. 📊 เพิ่มสถิติมวลรวมสะสมลงอีกตารางนึง (topic_stats)
    stats_collection.update_one(
        {"topic": topic},
        {"$inc": {"total_count": 1}, "$set": {"last_updated": current_utc}},
        upsert=True
    )

    print(f"✅ Processed & Aggregated: [{sentiment_status} | {topic}] '{text}'")