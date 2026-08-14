import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# ---------------------------------------------------------
# เชื่อมต่อ Kafka Producer พร้อม Auto-Retry
# ---------------------------------------------------------
print("⏳ Connecting to Kafka Producer...")
producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            api_version=(2, 0, 0),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        print("✅ Connected to Kafka Producer successfully!")
    except Exception:
        print("⏳ Waiting for Kafka Broker... (retrying in 3 seconds)")
        time.sleep(3)

TOPIC_NAME = 'banking_feedback'

# 📌 ชุดข้อมูลคอมเมนต์เชิงบวก (Positive) และทั่วไป (Neutral) สำหรับสภาวะปกติ
normal_comments = [
    # Positive
    "ใช้งานได้ลื่นไหล ไม่เคยเจอปัญหาล่มเลยครับ ให้ 5 ดาว",
    "มีอัปเดตฟังก์ชันใหม่ถือว่าทำได้ดีขึ้นครับ หน้าตาดูง่ายขึ้น",
    "ระบบรักษาความปลอดภัยดีเยี่ยม มีการยืนยันหลายขั้นตอน",
    "ระบบบันทึก Slip สวยงามและแชร์ให้เพื่อนได้สะดวกมาก",
    "ระบบจัดการบัตรเครดิตทำได้ดี จ่ายยอดแล้วยอดคืนทันที",
    "ปรับปรุงใหม่แล้วดีขึ้นมาก ไวขึ้น ไม่กระตุกเหมือนเมื่อก่อน",
    "ระบบมีความเสถียรสูงมาก ไม่เคยเจอแอปค้างหรือล่มเลย",
    
    # Neutral / Inquiries
    "วันนี้แอปเปิดให้บริการถึงกี่โมงครับ",
    "ต้องการติดต่อเจ้าหน้าที่ต้องกดตรงไหนครับ ในแอปหาไม่เจอ",
    "เพิ่มวงเงินโอนต่อวันสามารถทำในแอปได้เลยไหมครับ",
    "อยากให้มีฟังก์ชันแยกบัญชีออมเงินตามเป้าหมายครับ",
    "อยากให้มีฟังก์ชันสแกนคิวอาร์จากรูปภาพในคลังภาพได้ง่ายขึ้น"
]

# 📌 ชุดข้อมูลคอมเมนต์เชิงลบ (Negative) สำหรับสภาวะวิกฤต
crisis_comments = [
    "โอนเงินแล้วยอดไม่ตัด แต่ปลายทางไม่ได้เงิน ห่วยมากแก้ด้วย",
    "ระบบปรับปรุงบ่อยเกินไปแล้ว ช่วงเงินเดือนออกล่มตลอด",
    "แสกนหน้าสิบรอบก็ไม่ผ่าน ต้องไปยืนยันตัวตนที่ตู้ตลอด เซ็ง",
    "กดชำระเงินแล้วระบบค้าง พอกดใหม่กลายเป็นจ่ายซ้ำสองรอบ",
    "แอปห่วยมาก อัปเดตใหม่แล้วเข้าไม่ได้เลยขึ้น Error 503",
    "ระบบแจ้งเตือนขยะเยอะเกินไป ไม่อยากได้โฆษณาในแอป",
    "แอปปรับปรุงระบบบ่อยเกินไป กระทบการใช้งานประจำวัน"
]

current_state = "NORMAL" 
start_time = time.time()

print("🚀 Data Stream Simulator Running... (Press Ctrl+C to stop)")

try:
    while True:
        elapsed = time.time() - start_time
        
        # จำลองสภาวะ: เมื่อผ่านไป 15 วินาที ระบบเข้าสู่สภาวะวิกฤต (CRISIS)
        if elapsed > 15 and current_state == "NORMAL":
            current_state = "CRISIS"
            print("\n⚠️ [STATE CHANGED] -> Entering CRISIS_STATE (System Breakdown)\n")
            
        if current_state == "NORMAL":
            text = random.choice(normal_comments)
            sleep_time = random.uniform(2.5, 5.0) 
        else:
            text = random.choice(crisis_comments)
            sleep_time = random.uniform(1.0, 2.0) 
            
        payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "app_version": "v2.1.0",
            "text": text
        }
        
        producer.send(TOPIC_NAME, value=payload)
        producer.flush()
        
        print(f"📥 Sent: {payload['timestamp']} | State: {current_state} | Text: {text}")
        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("\n🛑 Data Stream Simulator stopped.")
finally:
    if producer:
        producer.close()