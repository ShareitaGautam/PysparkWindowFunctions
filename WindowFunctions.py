
# ------------------------------------------------------------
# PySpark Practice Script
# This script shows how to use:
# 1. DataFrames
# 2. Window Functions
# 3. Date Functions
# 4. Timestamp Functions
# Every step has comments explaining what is happening.
# ------------------------------------------------------------


# ------------------------------------------------------------
# Import required PySpark libraries
# ------------------------------------------------------------

# SparkSession is used to start and work with Spark
from pyspark.sql import SparkSession

# Import all Spark SQL functions like row_number, rank, etc.
from pyspark.sql.functions import *

# Import Window class used for window functions
from pyspark.sql.window import Window


# ------------------------------------------------------------
# This block makes sure the code runs only when we run this file
# ------------------------------------------------------------
if __name__ == "__main__":

    # ------------------------------------------------------------
    # Start Spark Session
    # SparkSession is the main entry point for using Spark
    # ------------------------------------------------------------
    spark = SparkSession.builder.appName("PySparkPractice").getOrCreate()


    # ------------------------------------------------------------
    # 1. CREATE A DATAFRAME
    # ------------------------------------------------------------

    # Sample employee data
    # Each row has: employee name, department, and salary
    simpleData = [
        ("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)
    ]

    # Column names for the DataFrame
    columns = ["employee_name", "department", "salary"]

    # Create DataFrame using Spark
    df = spark.createDataFrame(simpleData, columns)

    # Print the structure of the DataFrame (schema)
    # Shows column names and data types
    df.printSchema()

    # Show the actual data
    df.show()


    # ------------------------------------------------------------
    # 2. WINDOW FUNCTIONS
    # ------------------------------------------------------------

    # Define a window specification
    # partitionBy -> groups rows by department
    # orderBy -> sorts rows inside each department by salary
    windowSpec = Window.partitionBy("department").orderBy("salary")


    # ------------------------------------------------------------
    # row_number()
    # Assigns a unique number to each row in each department
    # ------------------------------------------------------------
    print("Row Number Function")
    df.withColumn("row_number", row_number().over(windowSpec)).show()


    # ------------------------------------------------------------
    # rank()
    # Assigns ranking to rows
    # If two values are the same, they get the same rank
    # But the next rank number is skipped
    # Example: 1,1,3
    # ------------------------------------------------------------
    print("Rank Function")
    df.withColumn("rank", rank().over(windowSpec)).show()


    # ------------------------------------------------------------
    # dense_rank()
    # Similar to rank but it DOES NOT skip numbers
    # Example: 1,1,2
    # ------------------------------------------------------------
    print("Dense Rank Function")
    df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()


    # ------------------------------------------------------------
    # lag()
    # Gets value from the previous row
    # Used to compare current row with previous row
    # ------------------------------------------------------------
    print("Lag Function (previous salary)")
    df.withColumn("previous_salary", lag("salary", 1).over(windowSpec)).show()


    # ------------------------------------------------------------
    # lead()
    # Gets value from the next row
    # Used to compare current row with next row
    # ------------------------------------------------------------
    print("Lead Function (next salary)")
    df.withColumn("next_salary", lead("salary", 1).over(windowSpec)).show()



    # ------------------------------------------------------------
    # 3. DATE FUNCTIONS
    # ------------------------------------------------------------

    # Create sample date data
    data = [
        ("1", "2020-02-01"),
        ("2", "2019-03-01"),
        ("3", "2021-03-01")
    ]

    # Create DataFrame for date operations
    df_dates = spark.createDataFrame(data, ["id", "input"])

    # Show the date data
    df_dates.show()


    # ------------------------------------------------------------
    # current_date()
    # Returns today's current date
    # ------------------------------------------------------------
    df_dates.select(current_date().alias("today")).show()


    # ------------------------------------------------------------
    # date_format()
    # Changes the display format of the date
    # Example: yyyy-MM-dd -> MM-dd-yyyy
    # ------------------------------------------------------------
    df_dates.select(
        col("input"),
        date_format(col("input"), "MM-dd-yyyy").alias("formatted_date")
    ).show()


    # ------------------------------------------------------------
    # to_date()
    # Converts a string into a proper DateType
    # Useful when input data is stored as text
    # ------------------------------------------------------------
    df_dates.select(
        col("input"),
        to_date(col("input"), "yyyy-MM-dd").alias("date_type")
    ).show()


    # ------------------------------------------------------------
    # datediff()
    # Calculates difference in days between two dates
    # Here we compare current date with the input date
    # ------------------------------------------------------------
    df_dates.select(
        col("input"),
        datediff(current_date(), col("input")).alias("days_difference")
    ).show()


    # ------------------------------------------------------------
    # months_between()
    # Calculates difference in months between two dates
    # ------------------------------------------------------------
    df_dates.select(
        col("input"),
        months_between(current_date(), col("input")).alias("months_difference")
    ).show()


    # ------------------------------------------------------------
    # add_months(), date_add(), date_sub()
    # Used to add or subtract dates
    # ------------------------------------------------------------
    df_dates.select(
        col("input"),

        # add 3 months to the date
        add_months(col("input"), 3).alias("add_3_months"),

        # subtract 3 months
        add_months(col("input"), -3).alias("subtract_3_months"),

        # add 4 days
        date_add(col("input"), 4).alias("add_4_days"),

        # subtract 4 days
        date_sub(col("input"), 4).alias("subtract_4_days")

    ).show()


    # ------------------------------------------------------------
    # Extract parts of a date
    # ------------------------------------------------------------
    df_dates.select(

        col("input"),

        # extract year
        year(col("input")).alias("year"),

        # extract month
        month(col("input")).alias("month"),

        # find next Sunday after the date
        next_day(col("input"), "Sunday").alias("next_sunday"),

        # find week number in the year
        weekofyear(col("input")).alias("week_number")

    ).show()


    # ------------------------------------------------------------
    # Day related functions
    # ------------------------------------------------------------
    df_dates.select(

        # day of the week (1-7)
        dayofweek(current_date()).alias("day_of_week"),

        # day number of the month
        dayofmonth(current_date()).alias("day_of_month"),

        # day number of the year
        dayofyear(current_date()).alias("day_of_year")

    ).show()



    # ------------------------------------------------------------
    # 4. TIMESTAMP FUNCTIONS
    # ------------------------------------------------------------

    # Create timestamp data
    timestamp_data = [
        ("1", "2020-02-01 11:01:19.06"),
        ("2", "2019-03-01 12:01:19.406"),
        ("3", "2021-03-01 12:01:19.406")
    ]

    # Create DataFrame for timestamp operations
    df_time = spark.createDataFrame(timestamp_data, ["id", "input"])

    df_time.show()


    # ------------------------------------------------------------
    # Extract hour, minute and second from timestamp
    # ------------------------------------------------------------
    df_time.select(

        col("input"),

        # extract hour
        hour(col("input")).alias("hour"),

        # extract minute
        minute(col("input")).alias("minute"),

        # extract second
        second(col("input")).alias("second")

    ).show()


    # ------------------------------------------------------------
    # current_timestamp()
    # Shows current date and time
    # ------------------------------------------------------------
    df_time.select(current_timestamp().alias("current_timestamp")).show()


    # ------------------------------------------------------------
    # Convert string into timestamp
    # ------------------------------------------------------------
    raw_time = [
        ("1", "02-01-2020 11 01 19 06"),
        ("2", "03-01-2019 12 01 19 406"),
        ("3", "03-01-2021 12 01 19 406")
    ]

    df_raw = spark.createDataFrame(raw_time, ["id", "input"])

    df_raw.select(

        col("input"),

        # convert the string to proper timestamp format
        to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("timestamp_value")

    ).show()


