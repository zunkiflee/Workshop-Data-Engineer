## Workshop Data Data Warehouse

**สิ่งที่เรียนรู้จาก Workshop Data Warehouse**

1. BigQuery
    - สร้าง Dataset ใน BigQuery 
    - วิธีการโหลด Data จาก bucket ไปเก็บบน BigQuery 3 วิธี 1.โหลดผ่าน Console 2.โหลดผ่าน bq command ด้วย BashOperator 3.โหลดผ่าน GCSToBigQeryOperator
2. Airflow
    - สร้าง Task bq command และ Task GCSToBigQeryOperator เพื่อใช้งาน BigQuery และนำข้อมูลไปเก็บแบบ Automatic
