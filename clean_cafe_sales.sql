/* ใน SQL project อันนี้จะโชว์ Skill แค่ขั้นตอนการ EDA และ Cleaning data นะครับ  */
-- สร้าง stag table เพื่อไม่ให้กระบทบกับข้อมูลต้นฉบับ
CREATE TABLE cafe_sales.clean_cafe AS 
select
	*
from
	cafe_sales.dirty_cafe_sales;

-- Rename column
-- เปลี่ยนชื่อคอรัม เป็นตัวพิมพ์เล็ก และแทนที่ whitespace ด้วย _
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Transaction ID" TO transaction_id;
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Item" TO item;
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Quantity" TO quantity;
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Price Per Unit" TO price_per_unit;
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Total Spent" TO total_spent;
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Payment Method" TO payment_type;
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Location" TO location;
ALTER TABLE cafe_sales.clean_cafe RENAME COLUMN "Transaction Date" TO date;

-- Check value ในแต่ละ field ก่อนนะครับ ว่ามี text หรือ Whitespace ไหม ก่อนที่จะเปลี่ยน Data type นะครับ

SELECT DISTINCT transaction_id
FROM cafe_sales.clean_cafe
WHERE transaction_id IS NULL
OR transaction_id = ('');

SELECT DISTINCT item
FROM cafe_sales.clean_cafe;

SELECT  DISTINCT  quantity 
FROM cafe_sales.clean_cafe;


SELECT DISTINCT  price_per_unit
FROM cafe_sales.clean_cafe;

SELECT DISTINCT total_spent
FROM cafe_sales.clean_cafe;

SELECT DISTINCT payment_type
FROM cafe_sales.clean_cafe;

SELECT DISTINCT location
FROM cafe_sales.clean_cafe;

SELECT DISTINCT date
FROM cafe_sales.clean_cafe
WHERE date ~* '[a-z].+'
OR date IS NULL
OR date = ('');

-- จากนั้น Clean ข้อมูลแต่ละคอรัม เพื่อไม่ให้เกิด error ในการเปลี่ยน Data type ครับ
-- ใช้ CAHE WHEN กับคอรัมตัวเลขในการแทนค่าที่ไม่เกี่ยวข้องกับข้อมูล เช่น text, whitespace, NULL เป็น 0 นะครับ
-- แต่ในส่วนของคอรัมที่เป็น text จะไม่เปลี่ยนแปลงค่า UNKNOWN กับ ERROR นะครับ ในเคสนี้จะเปลี่ยนแค่ค่า whitespace และ NULL เป็นค่า unknown นะครับ
-- ที่ไม่เปลี่ยน unkwnown และ error เป็นเพราะขั้นตอน analyze บางทีจะต้องใช้ข้อมูลเหล่่านี้ในการวิเคราะห์ด้วยครับ ทั้งนี้ขึ้นอยู่กับจุดประสงค์ในการใช้ข้อมลนะครับ ถ้าเป็นการ สร้าง Model ก็ควรลบออกครับ

UPDATE cafe_sales.clean_cafe
SET item = CASE 
    WHEN item  IN ('') THEN 'unknown'
    WHEN item IS NULL THEN 'unknown'
    ELSE LOWER(item)
END;

UPDATE cafe_sales.clean_cafe
SET quantity = CASE 
    WHEN quantity IN ('UNKNOWN', 'ERROR','') THEN '0'
    WHEN quantity IS NULL THEN '0'
    ELSE quantity
END;

UPDATE cafe_sales.clean_cafe
SET price_per_unit = CASE 
    WHEN price_per_unit IN ('UNKNOWN', 'ERROR','') THEN '0'
    WHEN price_per_unit IS NULL THEN '0'
    ELSE price_per_unit
END;

UPDATE cafe_sales.clean_cafe
SET total_spent = CASE 
    WHEN total_spent IN ('UNKNOWN', 'ERROR','') THEN '0'
    WHEN total_spent IS NULL THEN '0'
    ELSE total_spent
END;

UPDATE cafe_sales.clean_cafe
SET payment_type = CASE 
    WHEN payment_type = '' THEN 'unknown'
    WHEN payment_type IS NULL THEN 'unknown'
    ELSE LOWER(payment_type)
END;

UPDATE cafe_sales.clean_cafe
SET location = CASE 
    WHEN location IN ('','Unknown') THEN 'unknown'
    WHEN location IS NULL THEN 'unknown'
    ELSE LOWER(location)
END;

-- ในส่วนคอรัม date ก่อนที่จะเปลี่ยน data type เป็น date ต้องลบค่า Text ออกก่อนนะครับถึงจะสามารถเปลี่ยน data type ได้
-- ผมจะลบค่า NULL ออก 
 DELETE FROM cafe_sales.clean_cafe 
 WHERE LOWER(date) IN ('unknown','error','');

--Change Data type นะครับ (ที่ต้องใช้ REAL เพราะเป็นเลขทศนิยมนะครับ)
ALTER TABLE cafe_sales.clean_cafe ALTER COLUMN quantity TYPE INTEGER USING quantity::INTEGER;
ALTER TABLE cafe_sales.clean_cafe ALTER COLUMN price_per_unit TYPE REAL USING price_per_unit ::REAL;
ALTER TABLE cafe_sales.clean_cafe ALTER COLUMN total_spent TYPE REAL USING total_spent ::REAL;
ALTER TABLE cafe_sales.clean_cafe ALTER COLUMN date TYPE DATE USING date ::DATE;

-- Check Duplicate ด้วย Window function นะครับ โดยใช้ count ในแต่ละ row ที่มีข้อมูลไม่ซ้ำกันนะครับ ถ้าซ้ำมันจะมากกว่า 1
SELECT *
FROM (
SELECT *, COUNT(*) OVER(PARTITION BY transaction_id,item,quantity,price_per_unit,total_spent,payment_type,LOCATION,date) AS n_dup
FROM cafe_sales.clean_cafe)
WHERE n_dup >1;
 ; -- ถ้าไม่ขึ้นสักแถวแปลว่าไม่มี duplicate นะครับ
 
 -- รีวิวข้อมูลอีกรอบก่อนที่จะไปขั้น analyze และ visualize นะครับ
 SELECT DISTINCT transaction_id		
 FROM cafe_sales.clean_cafe
 WHERE transaction_id IS NULL
 	OR transaction_id = '';
 
 SELECT COUNT(DISTINCT item) n_item,
 		MIN(quantity) min_quantity,
 		MAX(quantity) max_quantity,
 		MIN(price_per_unit) min_price_per_unit,
 		MAX(price_per_unit) max_price_per_unit,
 		MIN(total_spent) min_total_spent,
 		MAX(total_spent) max_total_spent,
 		COUNT(DISTINCT payment_type) n_payment,
 		COUNT(DISTINCT location) n_location,
 		MIN(date) min_date,
 		MAX(date) max_date
 FROM cafe_sales.clean_cafe;
 
 -- ข้อมูลพร้อมสำหรับขั้นตอน Analysis และ Visualization แล้วครับ