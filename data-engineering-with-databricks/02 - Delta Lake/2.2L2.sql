-- Databricks notebook source
create table beans(name STRING, color STRING, grams FLOAT, delicious boolean)


-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into beans values
-- MAGIC ('pinto', 'brown', 1.5, true),
-- MAGIC ('green', 'green', 178.3, true),
-- MAGIC ('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

select * from beans;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 9, "The table should have 3 records"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "4", "Only 3 commits should have been made to the table"

-- COMMAND ----------

show table dbacademy_nayanjyoti_bhagawati_resideo_com_dewd_2_2l.beans;



-- COMMAND ----------

describe history dbacademy_nayanjyoti_bhagawati_resideo_com_dewd_2_2l.beans;

-- COMMAND ----------

restore dbacademy_nayanjyoti_bhagawati_resideo_com_dewd_2_2l.beans VERSION AS OF 0

-- COMMAND ----------

select * from dbacademy_nayanjyoti_bhagawati_resideo_com_dewd_2_2l.beans;

-- COMMAND ----------

describe extended student;

-- COMMAND ----------

describe history student

-- COMMAND ----------

select * from student

-- COMMAND ----------

insert into student values 
( 1, "florin",15.5),
( 2, "nayan",95.5);

-- COMMAND ----------

describe extended student;

-- COMMAND ----------

DESCRIBE DETAIL student

-- COMMAND ----------


