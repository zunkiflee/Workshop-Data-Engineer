## Workshop Data Collection

**สิ่งที่เรียนรู้จาก Workshop Data Collection**

1. ทำ ETL โดย E(Extract) ดึงข้อมูลจาก MySQL Database, REST API Python 
   - Config เชื่อมต่อกับ MySQL Database และ API โดยใช้ Package requests
   - List Table/Query Table ด้วยคำสั่ง cursor และใช้ SQL เพื่อ list ตารางออกมา
   - Query ข้อมูลด้วย Pandas คำสั่ง read_sql()
2. T(Transform) ปรับรูปแบบให้เหมาะสมใช้ Pandas เพื่อจัดการข้อมูลที่ดึงมาง่ายขึ้น
   - แปลง Data จาก MySQL Database ที่อยู่ในรูปแบบ list ให้เป็น DataFrame และข้อมูลจาก REST API Python แบบ json เปลี่ยนเป็น DataFrame
   - join ข้อมูล DataFrame ที่ดึงจาก MySQL Database มี 2 Table คือ audible_data, audible_transaction ด้วยคำสั่ง merge() เก็บในตัวแปร transaction
   - ลบตัวอักษรที่ไม่ต้องการออกจากคอลัมน์ Price
   - เปลี่ยน Data Type ให้ถูกต้อง
   - สร้าง Function สำหรับคำนวนราคา
   - ใช้คำสั่งฟังก์ชันต่างๆ ใน Pandas เช่น query, filter, summary, rename, groupby สำหรับดูข้อมูลหนังสือ
3. Save ข้อมูลที่สมบูรณ์