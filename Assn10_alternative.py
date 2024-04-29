import findspark
findspark.init()

from pyspark.sql import SparkSession
import seaborn as sns

# Create a SparkSession
spark = SparkSession.builder \
    .appName("TitanicAnalysis") \
    .getOrCreate()

# Load the Titanic dataset
titanic_df = spark.createDataFrame(sns.load_dataset("titanic"))

titanic_df = titanic_df.fillna({'Age': 0})

# Filter data for male passengers who died and remove null values from Age column
male_deceased = titanic_df.filter((titanic_df["Sex"] == "male") & (titanic_df["Survived"] == 0) & titanic_df["Age"].isNotNull())

# Check if there are any male passengers who died
male_deceased_count = male_deceased.count()

if male_deceased_count > 0:
    # Calculate the average age of male passengers who died
# Calculate the average age of male passengers who died
    male_deceased_age_avg = male_deceased.agg({"Age": "avg"}).collect()[0][0]
    print("Number of male passengers who died:", male_deceased_count)
    print("Average age of male passengers who died:", male_deceased_age_avg)
else:
    print("No male passengers found who died in the dataset.")
    
female_deceased_by_class = titanic_df.filter((titanic_df["Sex"] == "female") & (titanic_df["Survived"] == 0)).groupBy("Pclass").count()

# Display results
print("Number of deceased passengers in each class among females:")
female_deceased_by_class.show()
# Stop SparkSession
spark.stop()