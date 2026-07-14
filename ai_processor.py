import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# 1. ตั้งค่าฝั่งดักฟังข้อมูล (Kafka Consumer) ให้ดักฟังท่อ 'banking_feedback'
consumer = KafkaConsumer(
    'banking_feedback',
    bootstrap_servers=['127.0.0.1:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# 2. เชื่อมต่อฐานข้อมูล MongoDB ที่รันอยู่ใน Docker
db_client = MongoClient('mongodb://localhost:27017/')
db = db_client['banking_metrics']
collection = db['feedback_data']

print("🤖 AI Processor Pipeline Standing By... (Waiting for Data)")

# 3. ลูปดักรับข้อมูลเมื่อมีข้อความไหลเข้าท่อ Kafka
for message in consumer:
    data = message.value
    text = data['text']
    
    # 🌟 จุดประยุกต์ใช้ AI/NLP (Quantification)
    # จำลองการทำงานของโมเดล AI ในการคำนวณสถิติและพ่นค่า Probability Score ออกมา
    if "ล่ม" in text or "ค้าง" in text:
        neg_score = 0.95
        sentiment = "Negative"
        topic = "System Down"
    elif "เด้ง" in text or "หมุน" in text:
        neg_score = 0.88
        sentiment = "Negative"
        topic = "App Crash"
    else:
        neg_score = 0.05
        sentiment = "Positive"
        topic = "General"
        
    # อัปเดตข้อมูลดิบด้วยค่าตัวเลขคณิตศาสตร์ที่ได้จาก AI
    data['sentiment'] = sentiment
    data['negative_score'] = neg_score
    data['topic'] = topic
    
    # บันทึกลงฐานข้อมูล MongoDB
    collection.insert_one(data)
    print(f"✅ AI Processed & Saved: {text} | Topic: {topic} | Neg Score: {neg_score}")