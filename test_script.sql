-- SQL 测试脚本（扩展示例）
-- 目的：演示 SQL 的三类常用语句：DDL（定义）、DML（数据操作）、DQL（查询）

-- =====================================================
-- 1) SQL 解析（简要说明）
-- =====================================================
-- DDL（Data Definition Language）：用于定义或修改数据库结构，常见语句：CREATE, DROP, ALTER
-- DML（Data Manipulation Language）：用于增删改数据记录，常见语句：INSERT, UPDATE, DELETE
-- DQL（Data Query Language）：用于查询数据，主要是 SELECT（有时也归类为 DML 的一部分）
-- 以下示例采用 MySQL 语法，按功能模块依次给出示例。

-- =====================================================
-- 2) DDL：数据库 / 表 的创建、删除与表结构修改示例
-- =====================================================
-- 创建数据库（若不存在则创建）
CREATE DATABASE IF NOT EXISTS demo_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE demo_db;

-- 如果已存在则先删除（演示用，慎用）
-- DROP DATABASE IF EXISTS demo_db;

-- 创建表：employees
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  age INT DEFAULT NULL,
  dept VARCHAR(50),
  email VARCHAR(150) UNIQUE,
  salary DECIMAL(10,2) DEFAULT 0.00,
  hire_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 添加列（示例：在 email 后增加 phone）
ALTER TABLE employees ADD COLUMN phone VARCHAR(30) AFTER email;

-- 修改列类型或约束（示例：将 age 改为 SMALLINT 并设置默认值/非空）
-- 注意：若表中已有 NULL 值，需要先处理数据或允许 NULL
ALTER TABLE employees MODIFY COLUMN age SMALLINT DEFAULT 0 NOT NULL;

-- 重命名列（MySQL 8.0+）
ALTER TABLE employees RENAME COLUMN dept TO department;

-- 旧语法同时改名和修改类型（兼容性示例）
-- ALTER TABLE employees CHANGE COLUMN email contact_email VARCHAR(200);

-- 删除列
ALTER TABLE employees DROP COLUMN phone;

-- 创建一个示例表并重命名（示例：表重命名）
DROP TABLE IF EXISTS projects;
CREATE TABLE projects (
  pid INT AUTO_INCREMENT PRIMARY KEY,
  pname VARCHAR(100) NOT NULL
);
RENAME TABLE projects TO project_list;

-- 删除表示例（按需启用）
-- DROP TABLE IF EXISTS project_list;

-- =====================================================
-- 3) DML：插入 / 更新 / 删除 记录 示例
-- =====================================================
-- 插入单条记录
INSERT INTO employees (name, age, department, email, salary, hire_date)
VALUES ('Alice', 30, '研发', 'alice@example.com', 8000.00, '2020-03-15');

-- 插入多条记录
INSERT INTO employees (name, age, department, email, salary, hire_date) VALUES
('Bob', 25, '销售', 'bob@example.com', 5500.00, '2021-07-01'),
('Charlie', 28, '产品', 'charlie@example.com', 6000.00, '2019-11-20');

-- 从其他表插入（示例：INSERT ... SELECT）
-- INSERT INTO employees (name, age, department)
-- SELECT name, age, dept FROM temp_import;

-- 更新记录（单列）
UPDATE employees SET department = '人力资源' WHERE name = 'Bob';

-- 更新记录（多列）
UPDATE employees SET salary = salary * 1.05, age = age + 1 WHERE hire_date < '2020-01-01';

-- 删除记录（带条件）
DELETE FROM employees WHERE age < 26;

-- 删除所有记录（谨慎）：TRUNCATE 会重置自增计数
-- TRUNCATE TABLE employees;

-- 事务示例：批量插入并在出现问题时回滚
START TRANSACTION;
INSERT INTO employees (name, age, department, email, salary, hire_date)
VALUES ('Dave', 32, '研发', 'dave@example.com', 9000.00, '2022-02-01');
UPDATE employees SET salary = salary + 500 WHERE name = 'Alice';
-- 若一切正常则提交：
COMMIT;
-- 若需回滚则使用：ROLLBACK;

-- =====================================================
-- 4) DQL：查询示例（简单条件）
-- =====================================================
-- 查询所有记录
SELECT * FROM employees;

-- 指定字段的所有记录
SELECT id, name, email, salary FROM employees;

-- 指定字段和 WHERE 条件
SELECT name, department FROM employees WHERE salary > 6000;

-- 组合条件查询
SELECT * FROM employees
WHERE department = '研发' AND hire_date BETWEEN '2019-01-01' AND '2023-01-01';

-- 模糊匹配
SELECT name, email FROM employees WHERE email LIKE '%@example.com';

-- IN与排序
SELECT id, name, salary FROM employees
WHERE department IN ('研发','产品')
ORDER BY salary DESC;

-- 聚合查询（分组）
SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING cnt > 0;

-- 查询后修改记录（查询用于确认，再执行更新）
SELECT * FROM employees WHERE name = 'Alice';
UPDATE employees SET salary = 8600.00 WHERE name = 'Alice';
SELECT * FROM employees WHERE name = 'Alice';

-- 清理语句（按需启用）
-- DROP TABLE IF EXISTS employees;
-- DROP DATABASE IF EXISTS demo_db;

-- 脚本结束
