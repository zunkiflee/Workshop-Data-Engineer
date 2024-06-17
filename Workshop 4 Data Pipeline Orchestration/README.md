## Workshop Data Pipeline Orchestration

**สิ่งที่เรียนรู้จาก Workshop Data Pipeline Orchestration**

1. วิธีการจัดการ Data Pipeline ให้ข้อมูลไหลตามขั้นตอน
   - วิธีสร้าง DAG 5 ขั้นตอน
   - สร้าง Task ว่าให้ทำงานส่วนไหนบ้างและส่ง argument ไปยัง Fucntion ที่สั่งให้แต่ละ Function ทำอะไรบ้าง
   - Operator ที่ใช้ใน Task ใช้ PythonOperator และ BashOperator
   - Schedule Interval ตั้งเวลาให้ Task รัน Job ใน Workshop นี้ตั้งเวลาเป็น None ไม่ทำงานอัตโนมัติ ถ้าให้ทำงานต้องกด Trigger DAG 
   - ใช้ providers MySqlHook ของ Airflow ในการต่อ MySQL
2. Cloud Composer 
   - สร้าง Composer สำหรับใช้งาน Airflow บน Google Cloud
   - เข้าไปจัดการ environment บน Composer เช่น ติดตั้ง python package 
   - gsutil สำหรับ copy ไฟล์ Pipeline ไปไว้ใน bucket เพื่อทำงานบน Airflow ได้
3. วิธีการทำ ETL ใน DAG นำ Code Python จาก Workshop 1 Data Collection ใส่ใน function 
4. Airflow 
   - ตั้งค่าเชื่อมต่อกับ MySQL Database สำหรับดึงข้อมูล
   - การใช้งาน Airflow Web UI เช่น DAGs แสดงการทำงาน Job ทั้งหมดที่รัน, เข้าไปดูการ DAG แสดงรายละเอียดมากขึ้นเช่น Tree View, Graph View, Code แสดง Code ที่เขียน, Log แสดง Log Info สามารถูว่า Error หรือทำงานได้ถูกต้อง
   - การดู Status การทำงานของ Job ติดขัดตรงไหนบ้าง