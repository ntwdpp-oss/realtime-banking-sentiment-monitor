import json
import time
from datetime import datetime, timezone
import torch
from kafka import KafkaConsumer
from pymongo import MongoClient
from transformers import pipeline

# ---------------------------------------------------------
# 1. เชื่อมต่อฐานข้อมูล MongoDB พร้อม Auto-Retry & Indexes
# ---------------------------------------------------------
print("⏳ Connecting to MongoDB...")
mongo_client = None

while mongo_client is None:
    try:
        mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        mongo_client.server_info()  # ทดสอบ ping
        db = mongo_client['banking_metrics']
        collection = db['feedback_data']
        summary_collection = db['topic_summary']
        
        # ทำ Index เพื่อให้ Dashboard คิวรีได้เร็วระดับ Real-time
        collection.create_index([("_id", -1)])
        collection.create_index([("timestamp", -1)])
        collection.create_index([("topic", 1)])
        print("✅ Connected to MongoDB (DB: banking_metrics) & Indexes configured.")
    except Exception as e:
        print(f"⏳ Waiting for MongoDB... Error: {e} (retrying in 3s)")
        time.sleep(3)

# ---------------------------------------------------------
# 2. โหลดโมเดล AI (รองรับ GPU อัตโนมัติ)
# ---------------------------------------------------------
device_id = 0 if torch.cuda.is_available() else -1
device_name = "GPU (CUDA)" if device_id == 0 else "CPU"
print(f"⚙️ Running AI Models on: {device_name}")

print("⏳ Loading CardiffNLP (XLM-RoBERTa) Sentiment Model...")
sentiment_pipeline = pipeline(
    "text-classification",
    model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
    device=device_id
)

print("⏳ Loading Zero-Shot Topic Classifier Model...")
topic_pipeline = pipeline(
    "zero-shot-classification",
    model="facebook/bart-large-mnli",
    device=device_id
)
print("✅ All AI Models Loaded Successfully.")

# ---------------------------------------------------------
# 3. กำหนด 6 หมวดหมู่ระบบที่เป็นมาตรฐานกลาง (Domain-Specific)
# ---------------------------------------------------------
# 1. System & Server Availability  (ความพร้อมใช้งานของระบบ / ล่ม)
# 2. App Stability & Crash         (ความเสถียรของแอปพลิเคชัน / เด้ง / ค้าง)
# 3. Transaction & Payment         (ระบบโอนเงิน / สแกนจ่าย / ตัดบัญชี)
# 4. Performance & Response Speed  (ความเร็วการตอบสนอง / ความลื่นไหล)
# 5. UI/UX & Application Design    (การออกแบบเมนู / ความสะดวก / สวยงาม)
# 6. General Inquiry & Support     (การสอบถามทั่วไป / บริการลูกค้า)

def classify_topic(text_message):
    text_lower = text_message.lower()
    
    # 3.1 ตรวจจับกรณีคำปฏิเสธ (Negation Check) เพื่อไม่ให้สับสนระหว่างคำชมกับปัญหา
    has_negation = any(neg in text_lower for neg in ["ไม่เคย", "ไม่มี", "ไม่เจอปัญหา", "ไม่หลุด", "ไม่ค้าง", "ไม่ล่ม"])
    
    # 3.2 กฎ Keyword Rule-Based ประสิทธิภาพสูง (พร้อม Negation Filtering)
    if any(k in text_lower for k in ["โอน", "สแกน", "พร้อมเพย์", "ตัดเงิน", "ยอดเงิน", "slip", "สลิป", "บัตรเครดิต"]):
        return "Transaction & Payment"
    
    if any(k in text_lower for k in ["ล่ม", "เซิร์ฟเวอร์", "เชื่อมต่อไม่ได้", "เข้าไม่ได้", "error 503"]):
        return "System & Server Availability"
    
    if any(k in text_lower for k in ["เด้ง", "ค้าง", "หมุน", "แอปปิด", "ดับ"]):
        if not has_negation:
            return "App Stability & Crash"
        else:
            return "App Stability & Crash"  # จัดหมวดหมู่เสถียรภาพ แต่ sentiment จะเป็นตัวชี้วัดว่าเป็นคำชม
            
    if any(k in text_lower for k in ["ช้า", "ไว", "เร็ว", "ลื่น", "กระตุก", "โหลดนาน", "แป๊บเดียว"]):
        return "Performance & Response Speed"
        
    if any(k in text_lower for k in ["ดีไซน์", "หน้าตา", "สวย", "เมนู", "ปุ่ม", "ใช้งานง่าย", "ใช้งานยาก", "สับสน", "งง"]):
        return "UI/UX & Application Design"

    # 3.3 Zero-Shot Fallback หากไม่ตรงกับ Keyword ใดๆ
    try:
        candidate_labels = [
            "การให้บริการระบบและเซิร์ฟเวอร์",
            "ความเสถียรของแอปพลิเคชันและการค้าง",
            "การโอนเงินชำระเงินและธุรกรรม",
            "ความเร็วและการตอบสนองของระบบ",
            "การออกแบบเมนูและความยากง่ายในการใช้งาน",
            "การสอบถามข้อมูลและการบริการลูกค้า"
        ]
        
        res = topic_pipeline(
            text_message,
            candidate_labels=candidate_labels,
            hypothesis_template="ข้อความนี้เกี่ยวข้องกับประเด็น {}"
        )
        best_label = res['labels'][0]
        
        label_map = {
            "การให้บริการระบบและเซิร์ฟเวอร์": "System & Server Availability",
            "ความเสถียรของแอปพลิเคชันและการค้าง": "App Stability & Crash",
            "การโอนเงินชำระเงินและธุรกรรม": "Transaction & Payment",
            "ความเร็วและการตอบสนองของระบบ": "Performance & Response Speed",
            "การออกแบบเมนูและความยากง่ายในการใช้งาน": "UI/UX & Application Design",
            "การสอบถามข้อมูลและการบริการลูกค้า": "General Inquiry & Support"
        }
        return label_map.get(best_label, "General Inquiry & Support")
    except Exception:
        return "General Inquiry & Support"

# ---------------------------------------------------------
# 4. เชื่อมต่อ Kafka Consumer พร้อม Auto-Retry
# ---------------------------------------------------------
print("⏳ Connecting to Kafka Consumer...")
consumer = None

while consumer is None:
    try:
        consumer = KafkaConsumer(
            'banking_feedback',
            bootstrap_servers=['127.0.0.1:9092'],
            api_version=(2, 0, 0),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("✅ Connected to Kafka Broker successfully!")
    except Exception:
        print("⏳ Waiting for Kafka Broker... (retrying in 3s)")
        time.sleep(3)

print("👂 AI Pipeline Listening for real-time feedback messages...")

# ---------------------------------------------------------
# 5. ประมวลผลข้อความต่อเนื่อง (Production Loop)
# ---------------------------------------------------------
try:
    for message in consumer:
        data = message.value
        text = data.get('text', '').strip()
        
        if not text:
            continue

        # --- 5.1 วิเคราะห์ Sentiment ผ่าน CardiffNLP ---
        try:
            sentiment_res = sentiment_pipeline(text)[0]
            raw_label = str(sentiment_res['label']).lower()
            confidence_score = float(sentiment_res['score'])
            
            # Map Label (positive, negative, neutral)
            if 'positive' in raw_label or 'label_2' in raw_label or 'pos' in raw_label:
                sentiment = "Positive"
                negative_score = round(1.0 - confidence_score, 4)
            elif 'negative' in raw_label or 'label_0' in raw_label or 'neg' in raw_label:
                sentiment = "Negative"
                negative_score = round(confidence_score, 4)
            else:
                sentiment = "Neutral"
                negative_score = 0.50
        except Exception as e:
            print(f"⚠️ Sentiment Pipeline Exception: {e}")
            sentiment, negative_score, confidence_score = "Neutral", 0.50, 0.0

        # --- 5.2 วิเคราะห์ Topic ---
        topic = classify_topic(text)

        # จัดเตรียมข้อมูลลง MongoDB
        data['sentiment'] = sentiment
        data['negative_score'] = negative_score
        data['confidence'] = confidence_score
        data['topic'] = topic
        data['processed_at'] = datetime.now(timezone.utc)
        
        # 1. บันทึกลง Collection ข้อมูลดิบ
        collection.insert_one(data)
        
        # 2. บันทึกยอดสะสมลง Collection สรุป (สำหรับ BI / Analytics)
        summary_collection.update_one(
            {"topic": topic},
            {
                "$inc": {
                    "total_count": 1,
                    f"{sentiment.lower()}_count": 1
                },
                "$set": {
                    "last_updated": datetime.now(timezone.utc)
                }
            },
            upsert=True
        )
        
        print(f"✅ [{sentiment.upper()}] (Neg: {negative_score:.2f}) [{topic}] : {text}")

except KeyboardInterrupt:
    print("\n🛑 AI Processor shutting down gracefully...")
finally:
    if consumer:
        consumer.close()
    if mongo_client:
        mongo_client.close()