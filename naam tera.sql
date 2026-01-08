CREATE DATABASE ETL_DB;
USE ETL_DB;
CREATE TABLE Sales_Transactions (
    Txn_ID INT,
    Customer_ID VARCHAR(10),
    Customer_Name VARCHAR(50),
    Product_ID VARCHAR(10),
    Quantity INT,
    Txn_Amount INT,
    Txn_Date DATE,
    City VARCHAR(50)
);
INSERT INTO Sales_Transactions VALUES
(201,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai'),
(202,'C102','Anjali Rao','P12',1,1500,'2025-12-01','Bengaluru'),
(203,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai'),
(204,'C103','Suresh Iyer','P13',3,6000,'2025-12-02','Chennai'),
(205,'C104','Neha Singh','P14',NULL,2500,'2025-12-02','Delhi'),
(206,'C105','N/A','P15',1,NULL,'2025-12-03','Pune'),
(207,'C106','Amit Verma','P16',1,1800,NULL,'Pune'),
(208,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai');

CREATE TABLE Customers_Master (
    CustomerID VARCHAR(10),
    CustomerName VARCHAR(50),
    City VARCHAR(50)
);
INSERT INTO Customers_Master VALUES
('C101','Rahul Mehta','Mumbai'),
('C102','Anjali Rao','Bengaluru'),
('C103','Suresh Iyer','Chennai'),
('C104','Neha Singh','Delhi');

SELECT * FROM Sales_Transactions;
SELECT * FROM Customers_Master;

-- Question 1 : Define Data Quality in the context of ETL pipelines. Why is it more than just data cleaning? 
SELECT *
FROM Sales_Transactions
WHERE Customer_ID IS NULL
   OR Product_ID IS NULL
   OR Txn_Date IS NULL
   OR Txn_Amount IS NULL;

-- Question 2 : Explain why poor data quality leads to misleading dashboards and incorrect decisions.
SELECT *
FROM Sales_Transactions
WHERE Txn_Amount IS NULL
   OR Txn_Date IS NULL;

-- Question 3 : What is duplicate data? Explain three causes in ETL pipelines.
SELECT
    Customer_ID,
    Product_ID,
    Txn_Date,
    COUNT(*) AS duplicate_count
FROM Sales_Transactions
GROUP BY Customer_ID, Product_ID, Txn_Date
HAVING COUNT(*) > 1;

-- Question 4 : Differentiate between exact, partial, and fuzzy duplicates.
SELECT
    Customer_ID,
    Product_ID,
    Txn_Date,
    Txn_Amount,
    COUNT(*) AS exact_duplicates
FROM Sales_Transactions
GROUP BY Customer_ID, Product_ID, Txn_Date, Txn_Amount
HAVING COUNT(*) > 1;

-- Question 5 : Why should data validation be performed during transformation rather than after loading?
SELECT *
FROM Sales_Transactions
WHERE Quantity IS NULL
   OR Quantity <= 0
   OR Txn_Amount <= 0
   OR Txn_Amount IS NULL;
   
-- Question 6 : Explain how business rules help in validating data accuracy. Give an example.
SELECT *
FROM Sales_Transactions
WHERE Quantity IS NULL
   OR Txn_Amount IS NULL;
   
 --  Question 7 : Write an SQL query on Sales_Transactions to list all duplicate keys and their counts using the business key (Customer_ID + Product_ID + Txn_Date + Txn_Amount )
 SELECT
    Customer_ID,
    Product_ID,
    Txn_Date,
    Txn_Amount,
    COUNT(*) AS duplicate_count
FROM Sales_Transactions
GROUP BY
    Customer_ID,
    Product_ID,
    Txn_Date,
    Txn_Amount
HAVING COUNT(*) > 1;

-- Question 8 : Enforcing Referential Integrity Assume the following Customers_Master table:
-- Identify Sales_Transactions.Customer_ID values that violate referential integrity when joined with
-- Customers_Master and write a query to detect such violations.
SELECT
    s.Txn_ID,
    s.Customer_ID
FROM Sales_Transactions s
LEFT JOIN Customers_Master c
ON s.Customer_ID = c.CustomerID
WHERE c.CustomerID IS NULL;
