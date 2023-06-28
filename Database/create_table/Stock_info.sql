CREATE TABLE Stock_info (
  Stock_id VARCHAR(6) PRIMARY KEY,
  Stock_name VARCHAR(10) NOT NULL,
  Industry_id VARCHAR(2) NOT NULL,
  Industry_name VARCHAR(10) NOT NULL,
  List_type VARCHAR(2) NOT NULL,
  List_status VARCHAR(2) NOT NULL
);
