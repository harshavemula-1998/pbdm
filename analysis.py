from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring_index, desc  # Import desc function
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("COVID-19 Analysis") \
    .getOrCreate()

# Prompt user to input the filename
folder_path = "./csse_covid_19_daily_reports_us"
file_name = input("Enter the filename (mm-dd-yyyy.csv): ").strip()  # Remove leading/trailing whitespace
file_path = os.path.join(folder_path, file_name)
print(file_path)

# Load COVID-19 dataset
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Calculate Monthly Infection Fatality Ratio (IFR)
df = df.withColumn("IFR", (col("Deaths") / col("Confirmed")) * 100)

# Calculate IFR for each state
state_ifr = df.groupBy("Province_State").agg({"IFR": "avg"}).withColumnRenamed("avg(IFR)", "IFR")

# Display top 10 states with highest IFR
top_10_states = state_ifr.orderBy(desc("IFR")).limit(10)
print("Top 10 States with Highest Infection Fatality Ratio (IFR):")
top_10_states.show()
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

# List of states you want to color
# Assuming top_10_states is a DataFrame containing the 'Province_State' column
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Assuming you already have df, state_ifr, and top_10_states DataFrames created as mentioned in your code

# Collect 'Province_State' column from top_10_states DataFrame into a list
states_to_color = [row['Province_State'] for row in top_10_states.collect()]

print("States to color:", states_to_color)

# Filter out unknown states
known_states_to_color = [state for state in states_to_color if state in state_abbr]

# Create the figure
fig = go.Figure()

# Add the choropleth map of the United States without specifying values
color_intensity = 0.8  # Initial color intensity
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


spark.stop()