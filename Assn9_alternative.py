import findspark
findspark.init()

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("StudentGrades") \
    .getOrCreate()

# Sample student scores
scores = [
    ("Alice", {"Math": 85, "Science": 90, "English": 80}),
    ("Bob", {"Math": 70, "Science": 75, "English": 85}),
    ("Charlie", {"Math": 60, "Science": 65, "English": 70}),
    ("David", {"Math": 90, "Science": 95, "English": 85}),
    ("Eve", {"Math": 75, "Science": 80, "English": 75})
]

# Create RDD from the scores
scores_rdd = spark.sparkContext.parallelize(scores)

# Define the grading scheme (example)
grading_scheme = {
    "A": (80, 100),
    "B": (60, 79),
    "C": (40, 59),
    "D": (0, 39)
}

# Function to compute grades for a given score
def compute_grade(score):
    for grade, (lower_bound, upper_bound) in grading_scheme.items():
        if lower_bound <= score <= upper_bound:
            return grade
    return "F"

# Map operation to compute grades for each student
grades_rdd = scores_rdd.map(lambda x: (x[0], {subject: compute_grade(score) for subject, score in x[1].items()}))

# Convert RDD to DataFrame
grades_df = spark.createDataFrame(grades_rdd.flatMap(lambda x: [(x[0], subject, grade) for subject, grade in x[1].items()]), ["Student", "Subject", "Grade"])

# Display the result
grades_df.show()

# Stop SparkSession
spark.stop()