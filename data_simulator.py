import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# 🟢 1. คำชมเชย (Positive Feedback)
positive_comments = [
    "แอปปรับปรุงใหม่โอนไวขึ้นเยอะเลย ชอบมากครับ",
    "สแกนจ่ายสะดวกมาก หน้าตาแอปใหม่สวยสบายตาดี",
    "โอนเงินสะดวกดี ไม่เคยเจอระบบล่มเลย ประทับใจ",
    "ฟีเจอร์ใหม่ดีงาม สแกนหน้าติดไวมากครับ",
    "แอปใช้งานง่าย เหมาะกับคนแก่มาก เข้าใจง่ายดี",
    "บริการดี โอนเงินไว ระบบเสถียรมากช่วงนี้"
]

# ⚪ 2. คำถามหรือข้อเสนอแนะทั่วไป (Neutral / General Enquiries)
neutral_comments = [
    "สอบถามหน่อยครับ ปรับวงเงินโอนต่อวันทำยังไง",
    "ขอสเตทเม้นผ่านแอปย้อนหลัง 6 เดือนใช้เวลากี่วันครับ",
    "สาขาธนาคารในห้างเปิดกี่โมงครับช่วงวันหยุด",
    "ถ้าเปลี่ยนเครื่องใหม่ ต้องไปยืนยันตัวตนที่ตู้ไหม",
    "อยากให้เพิ่มธีมหน้าจอแอปแบบใหม่ๆ เพิ่มเติมครับ"
]

# 🔴 3. คำบ่นตรงๆ (Direct Negative Feedback)
negative_comments = [
    "แอปเป็นอะไรอีกแล้ว โอนเงินไม่ได้ระบบขึ้นขัดข้อง!",
    "เด้งออกตลอดเวลา สแกนจ่ายเงินไม่ได้เลย หัวร้อนมาก",
    "เงินตัดไปแล้วแต่ปลายทางยังไม่ได้ ทำงานยังไงเนี่ย!!",
    "ระบบล่มวันสิ้นเดือนตลอด ห่วยแตกมากครับ",
    "เข้าแอปไม่ได้ค้างหน้าโลโก้มาสิบนาทีแล้ว ปรับปรุงด่วน"
]

# 😏 4. คำประชดประชันสุดแสบ (Sarcastic / Passive-Aggressive Comments)
sarcastic_comments = [
    "ขอบคุณที่ล่มตอนเที่ยงนะครับ กำลังจะจ่ายค่าข้าวพอดี อิ่มทิพย์เลยกู",
    "แอปธนาคารหรือเกมส์ตักไข่ครับเนี่ย ลุ้นทุกครั้งที่กดเข้าว่าจะเด้งไหม",
    "ระบบดีมากครับ หมุนตลอดยิ่งกว่าพัดลมที่บ้านอีก",
    "ขอบคุณที่ช่วยประหยัดเงินนะครับ โอนไม่ได้สักบาท สมถะเลยช่วงนี้",
    "แอปเสถียรมากครับ เสถียรว่าล่มทุกสิ้นเดือนไม่เคยพลาดเลยสักครั้ง",
    "พัฒนาแอปเก่งมากครับ พัฒนาให้แย่ลงเรื่อยๆ เอาใจไปเลย",
    "จะจ่ายเงินแม่ค้า หน้าแอปหมุนจนแม่ค้ามองหน้าแล้วครับ ประทับใจมาก",
    "ขอบคุณระบบความปลอดภัยครับ ปลอดภัยจนเจ้าของบัญชียังเข้าไม่ได้เลย",
    "เป็นแอปที่สอนให้เราฝึกสมาธิและความอดทนได้ดีจริงๆ ครับ"
]

print("🚀 Data Simulator Starting... Press Ctrl+C to stop.")
print("📡 Streaming diverse Thai comments to Kafka topic 'banking_feedback'...\n")

try:
    while True:
        # สุ่มสถานะของระบบ: 70% โหมดปกติ (ผสมชม-บ่น-ประชด), 30% โหมดวิกฤต (ประชดหนัก+บ่นรัว)
        is_crisis_period = random.random() < 0.30

        if is_crisis_period:
            print("🔥 [SIMULATOR STATUS] Entering Simulated High-Traffic Crisis Period!")
            # โหมดวิกฤต: สุ่มเอาคำบ่นตรงๆ และคำประชดประชันสลับกัน
            text = random.choice(negative_comments + sarcastic_comments)
            sleep_time = random.uniform(1.5, 3.0)  # ส่งถี่ขึ้นนิดหน่อย
        else:
            print("🟢 [SIMULATOR STATUS] Operating Normal Traffic Flow...")
            # โหมดปกติ: สุ่มจากทุกหมวดหมู่ (ชม, ทั่วไป, บ่น, ประชด)
            all_options = positive_comments * 3 + neutral_comments * 2 + negative_comments + sarcastic_comments
            text = random.choice(all_options)
            sleep_time = random.uniform(3.5, 6.0)  # จังหวะสบายๆ

        # สร้างข้อความข้อมูล JSON
        payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": f"USER_{random.randint(1000, 9999)}",
            "text": text,
            "app_version": random.choice(["v2.1.0", "v2.2.0"])
        }

        # ยิงข้อมูลลง Kafka Topic
        producer.send('banking_feedback', payload)
        print(f"📤 Sent: '{text}' (Next message in {sleep_time:.1f}s)")

        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("\n🛑 Data Simulator Stopped Successfully.")