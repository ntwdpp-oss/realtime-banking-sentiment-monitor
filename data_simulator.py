import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# เชื่อมต่อเข้า Kafka ใน Docker
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

normal_comments = ["โอนเงินไวมาก ชอบๆ", "หน้าตาแอปสวยขึ้นนะ", "สแกนจ่ายสะดวกดี", "แอปเสถียรดีครับ"]
crisis_comments = ["ระบบล่มหรือเปล่า โอนเงินไม่ไป", "หมุนอยู่นั่นแหละ สิ้นเดือนทีไรล่มตลอด", "แอปเด้งออกตลอดเลย เข้าไม่ได้!"]

current_state = "NORMAL" 
start_time = time.time()

print("🚀 Data Stream Simulator Running... (Press Ctrl+C to stop)")

while True:
    elapsed = time.time() - start_time
    # จำลองว่าผ่านไป 15 วินาที แล้วระบบธนาคารล่ม คอมเมนต์ด่าจะทะลักเข้าท่อข้อมูล
    if elapsed > 15 and current_state == "NORMAL":
        current_state = "CRISIS"
        print("\n⚠️ [STATE CHANGED] -> Entering CRISIS_STATE (System Breakdown)\n")
        
    if current_state == "NORMAL":
        text = random.choice(normal_comments)
        sleep_time = random.uniform(4.0, 8.0) 
    else:
        text = random.choice(crisis_comments)
        sleep_time = random.uniform(1.0, 2.0) 
        
    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "app_version": "v2.1.0",
        "text": text
    }
    
    producer.send('banking_feedback', value=payload)
    print(f"📥 Sent: {payload['timestamp']} | State: {current_state} | Text: {text}")
    time.sleep(sleep_time)