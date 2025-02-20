# Flight Delay and Cancellation Analysis using Apache Spark in Databricks

## Project Overview
This project analyzes flight delay and cancellation data using Apache Spark in Databricks. The dataset used for this project is from [Kaggle](https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023) and contains flight records from 2019 to 2023. The analysis covers data loading, cleaning, transformation, aggregation, and advanced queries using Spark SQL and DataFrames.

## Dataset Information
- **Source**: Kaggle - Flight Delay and Cancellation Dataset (2019-2023)
- **Number of Rows**: ~6 million
- **Number of Columns**: 30+ columns
- **Key Columns**:
  - `FL_DATE`: Flight date
  - `AIRLINE`, `AIRLINE_CODE`: Airline name and code
  - `ORIGIN`, `DEST`: Origin and destination airport codes
  - `DEP_DELAY`, `ARR_DELAY`: Departure and arrival delays
  - `CANCELLED`, `CANCELLATION_CODE`: Flight cancellation details
  - `DISTANCE`: Distance between origin and destination

## Tasks Performed
### 1. Data Loading and Exploration (RDD & DataFrame)
- Loaded the dataset into a Spark DataFrame and handled errors during loading.
- Converted the DataFrame to an RDD, performed operations (e.g., counting flights), and converted it back.
- Printed schema and identified column data types.

### 2. Data Cleaning and Transformation
- Handled missing values by replacing `NULL` values in `DEP_DELAY` and `ARR_DELAY` with `0`.
- Converted `FL_DATE` to a proper Date type.
- Created a new column `TOTAL_DELAY = DEP_DELAY + ARR_DELAY`.
- Filtered out cancelled flights.

### 3. Data Analysis and Aggregation
- Calculated average departure delay per airline and sorted in descending order.
- Found the top 5 most frequent origin-destination airport pairs.
- Computed the percentage of flights with departure delays >15 minutes.
- Calculated average arrival delay per destination airport.

### 4. Advanced Analysis
- Used window functions to calculate the running average of `TOTAL_DELAY` per airline over `FL_DATE`.
- Identified the airline with the highest number of flights in each month.

### 5. Saving Results
- Saved the results of the average departure delay by airline as a CSV file.

## Spark SQL Queries
### 1. Top 10 Airlines with the Most Cancelled Flights
```sql
SELECT AIRLINE, COUNT(*) AS CANCELLED_COUNT 
FROM flights 
WHERE CANCELLED = 1 
GROUP BY AIRLINE 
ORDER BY CANCELLED_COUNT DESC 
LIMIT 10;
```

### 2. Destination with Lowest Average Arrival Delay per Origin
```sql
SELECT ORIGIN, DEST, AVG(ARR_DELAY) AS AVG_ARR_DELAY 
FROM flights 
GROUP BY ORIGIN, DEST 
HAVING AVG_ARR_DELAY = (SELECT MIN(AVG_ARR_DELAY) FROM (SELECT ORIGIN, DEST, AVG(ARR_DELAY) AS AVG_ARR_DELAY FROM flights GROUP BY ORIGIN, DEST));
```

### 3. Airline with the Most Delayed Flights (>30 min Total Delay)
```sql
CREATE VIEW DelayedFlights AS 
SELECT * FROM flights WHERE TOTAL_DELAY > 30;

SELECT AIRLINE, COUNT(*) AS DELAYED_COUNT 
FROM DelayedFlights 
GROUP BY AIRLINE 
ORDER BY DELAYED_COUNT DESC 
LIMIT 1;
```

### 4. Flight Percentage by Origin and Destination
```sql
SELECT ORIGIN, DEST, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights) AS FLIGHT_PERCENTAGE 
FROM flights 
GROUP BY ORIGIN, DEST 
ORDER BY FLIGHT_PERCENTAGE DESC;
```

### 5. Top 3 Busiest Days of the Week
```sql
SELECT DAYNAME(FL_DATE) AS DAY_OF_WEEK, COUNT(*) AS FLIGHT_COUNT 
FROM flights 
GROUP BY DAYNAME(FL_DATE) 
ORDER BY FLIGHT_COUNT DESC 
LIMIT 3;
```

## DataFrame vs RDD: A Comparison
### **DataFrame**
- **Structured**: Follows a schema with named columns.
- **Optimized Execution**: Uses Catalyst optimizer for query planning.
- **Easier to Use**: Provides SQL-like operations with high-level API.
- **Performance**: Faster due to optimizations like predicate pushdown.

### **RDD (Resilient Distributed Dataset)**
- **Unstructured**: Data is stored as distributed collections of objects.
- **Manual Optimizations**: No automatic optimization like DataFrames.
- **Low-level API**: Requires more code for transformations.
- **Performance**: Slower due to lack of schema and optimizations.

### **Why DataFrames are Preferred?**
DataFrames are preferred over RDDs because they provide better performance, ease of use, and optimizations. They allow SQL-like querying, automatic schema handling, and are optimized by Spark's Catalyst engine.

## Challenges Faced
- Handling missing values efficiently.
- Optimizing queries to process millions of records.
- Understanding Spark's execution plan for better performance.
- Using window functions for advanced analysis.

## Conclusion
This project demonstrated the power of Apache Spark for big data analysis in Databricks. By leveraging DataFrames, Spark SQL, and RDDs, we efficiently analyzed flight delays and cancellations. The findings help in understanding airline performance, common delays, and optimizing operations.

## Future Work
- Implementing machine learning models to predict flight delays.
- Expanding analysis with additional datasets (e.g., weather conditions).
- Building a real-time dashboard using Spark Streaming.

## Author
**Riya Vora**

## License
This project is open-source and available for modification and use.

---
📌 *Feel free to fork and contribute!* 🚀

