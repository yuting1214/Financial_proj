# Select into
## The SELECT INTO statement copies data from one table into a new table
SELECT *
INTO newtable [IN externaldb]
FROM oldtable
WHERE condition;

# INSERT INTO
## The INSERT INTO SELECT statement copies data from one table and inserts it into another table.
INSERT INTO table2 (column1, column2, column3, ...)
SELECT column1, column2, column3, ...
FROM table1
WHERE condition;
