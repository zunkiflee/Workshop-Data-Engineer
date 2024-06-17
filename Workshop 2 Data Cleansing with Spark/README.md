## Workshop Data Cleansing with Spark

**สิ่งที่เรียนรู้จาก Workshop**

1. Setup Spark และ PySpark ดึงข้อมูลจาก Link ที่อยู่ในรูปแบบ zip และ Load ข้อมูลไปยัง Spark 
2. ตรวจสอบข้อมูลด้วย Data Profiling เช่น มีกี่คอลัมน์, Type ของคอลัมน์, นับจำนวนแถวและคอลัมน์
3. Exploratory Data Analysis(EDA) ดูรายละเอียด แบบตัวเลข เช่น ข้อมูลสถิติ และแบบกราฟฟิก เช่น Boxplot, Histogram, Scatterplot, JoinJPlot
4. ทำ Data Cleansing ด้วย Spark เช่น เปลี่ยน Type คอลัมน์ให้ถูกต้อง, เช็คความผิดปกติของ Data โดยรวม Syntactical Anomalies เช็คคำที่สะกดผิด Semantic Anomalies ข้อมูลที่ไม่ตรงตามเงื่อนไข Missing value หาค่า null และเปลี่ยนไม่เป็นค่าที่ว่างเปล่า Outliers
5. ใช้ Spark SQL ดึง Data จาก Spark DataFrame และทำ Data Cleansing จัดการ Missing value, หาความผิดปกติของ Data 
6. ใช้คำสั่ง select() when() withColumn() ตรวจสอบตามเงื่อนไขของข้อมูล
7. ใช้ Spark/Spark SQL ทำ ETL 
8. ใช้ Library Pandas จัดการ Data Cleansing ให้เหมือนกัน Spark 