# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, coalesce, avg, lit, desc
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("COVID-19 Analysis") \
    .getOrCreate()

# Define a dictionary to map month abbreviations to their MM format
month_mapping = {
    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
    'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
    'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
}

# Prompt user input for month abbreviation and year
month_abbrev = input("Enter the month abbreviation (e.g., 'jan' for January): ").strip().lower()
year = input("Enter the year (YYYY): ").strip()

# Convert month abbreviation to MM format
month_mm = month_mapping.get(month_abbrev)
if not month_mm:
    print("Invalid month abbreviation.")
    exit()

# Define the path to the folder containing CSV files
folder_path = "./csse_covid_19_daily_reports_us"

# List to store DataFrames for each day in the specified month
dfs = []

# Iterate over each day in the specified month and load the corresponding CSV file
for day in range(1, 32):  # Assuming each month has 31 days
    # Construct the file name for the current day
    file_name = f"{month_mm}-{day:02d}-{year}.csv"
    file_path = os.path.join(folder_path, file_name)

    # Check if the CSV file exists for the current day
    if os.path.exists(file_path):
        # Load COVID-19 dataset for the current day
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Calculate Infection Fatality Ratio (IFR) for each day
        df = df.withColumn("IFR", (col("Deaths") / col("Confirmed")) * 100)

        # Add DataFrame for the current day to the list
        dfs.append(df)

# Union all DataFrames
if dfs:
    # Initialize with the DataFrame for the first day
    january_df = dfs[0]
    for df in dfs[1:]:
        january_df = january_df.union(df)

    # Group by Province_State and aggregate the values of Deaths and Confirmed
    aggregated_df = january_df.groupby("Province_State") \
                               .agg(sum("Deaths").alias("Total_Deaths"), 
                                    sum("Confirmed").alias("Total_Confirmed"))

    # Calculate Infection Fatality Ratio (IFR) for the aggregated data
    aggregated_df = aggregated_df.withColumn("IFR", (col("Total_Deaths") / col("Total_Confirmed")) * 100)

    # Display top 10 states with highest IFR for the specified month
    top_10_states = aggregated_df.orderBy(desc("IFR")).limit(10)
    print(f"Top 10 States with Highest Infection Fatality Ratio (IFR) in {month_abbrev.capitalize()} {year}:")
    top_10_states.show()
else:
    print(f"No data available for {month_abbrev.capitalize()} {year}.")


# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Assuming you already have df, state_ifr, and top_10_states DataFrames created as mentioned in your code

# Collect 'Province_State' column from top_10_states DataFrame into a list
states_to_color = [row['Province_State'] for row in top_10_states.collect()]

print("States to color:", states_to_color)
import plotly.graph_objects as go

# Define the mapping from state names to abbreviations
state_abbr = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA',
    'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT',
    'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
}

known_states_to_color = [state for state in states_to_color if state in state_abbr]

# Create the figure
fig = go.Figure()

# Add the choropleth map of the United States without specifying values
color_intensity = 1.2 # Initial color intensity
for state in known_states_to_color:
    fig.add_trace(go.Choropleth(
        locationmode='USA-states',
        locations=[state_abbr[state]],  # State abbreviations
        z=[1],  # Assigning a value of 1 to each state
        colorscale=[[0, f'rgba(0, 0, 255, {color_intensity})'], [1, f'rgba(0, 0, 255, {color_intensity})']],  # Light blue color
        showscale=False,  # Do not show the color scale
    ))
    color_intensity -= 0.1  # Decrease color intensity for the next state

# Update the layout
fig.update_layout(
    title_text='Choropleth Map of US States',
    geo=dict(
        scope='usa',
        projection_type='albers usa',
        showlakes=True,
        lakecolor='rgb(255, 255, 255)',
    ),
)

# Show the figure
fig.show()

# Stop SparkSession
spark.stop()
