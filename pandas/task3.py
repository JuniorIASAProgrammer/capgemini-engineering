import pandas as pd


OUTPUT_PATH = 'pandas/time_series_airbnb_data.csv'
df = pd.read_csv('pandas/cleaned_airbnb_data.csv')


def set_availiability_status(n_days):
    if n_days < 50:
        return 'Rarely Available'
    elif 50 <= n_days < 200:
        return 'Occasionally Available'
    else:
        return 'Highly Available'
    
def print_analysis_results(df, message=None, shape=False, info=False):
    if message:
        print(f"----------------{message}---------------")
    if shape:
        print(f"Shape - {df.shape}")
    if info:
        print(f"Info:")
        df.info()
    print(df.head(), end="\n\n")


if __name__ == '__main__':
    #Use the pivot_table function to create a detailed summary that reveals the average price for different combinations of neighbourhood_group and room_type. This analysis will help identify high-demand areas and optimize pricing strategies across various types of accommodations (e.g., Entire home/apt vs. Private room).
    pivot_table = pd.pivot_table(data=df, index='neighbourhood_group', columns='room_type', values='price', aggfunc='mean')
    print_analysis_results(pivot_table, message='Pivot table')
    #Transform the dataset from a wide format to a long format using the melt function. This restructuring facilitates more flexible and detailed analysis of key metrics like price and minimum_nights, enabling the identification of trends, outliers, and correlations.
    melted_df = pd.melt(df, id_vars=['name'], value_vars=['price', 'minimum_nights'], 
                  var_name='metric', value_name='value')
    print_analysis_results(melted_df, message='Melted table')
    #Create a new column availability_status using the apply function, classifying each listing into one of three categories based on the availability_365 column
    df['availability_status'] = df['availability_365'].apply(set_availiability_status)
    #Analyze trends and patterns using the new availability_status column, and investigate potential correlations between availability and other key variables like price, number_of_reviews, and neighbourhood_group to uncover insights that could inform marketing and operational strategies.
    grouped_status_neighbour = df.groupby(['availability_status', 'neighbourhood_group'])[['price', 'number_of_reviews']].aggregate(['min', 'mean', 'median', 'max'])
    print_analysis_results(grouped_status_neighbour, message='Grouped by status and neighbourhood_group')
    #Perform basic descriptive statistics (e.g., mean, median, standard deviation) on numeric columns such as price, minimum_nights, and number_of_reviews to summarize the dataset's central tendencies and variability, which is crucial for understanding overall market dynamics.
    describe_df = df[['price', 'minimum_nights', 'number_of_reviews']].describe()
    print_analysis_results(describe_df, message='Descriptive statistics for price minimum_nights and number_of_reviews')
    #Convert the last_review column to a datetime object and set it as the index of the DataFrame to facilitate time-based analyses.
    df['last_review'] = pd.to_datetime(df['last_review'])
    df['month'] =  df['last_review'].dt.month
    df.set_index('last_review', inplace=True)
    #Resample the data to observe monthly trends in the number of reviews and average prices, providing insights into how demand and pricing fluctuate over time.
    monthly_trends = df.resample('M').aggregate({
        'price': 'mean',
        'number_of_reviews': 'sum'
    }).rename(columns={'price': 'avg_price', 'reviews': 'total_reviews'})
    print_analysis_results(monthly_trends, message='Trends across years')
    #Group the data by month to calculate monthly averages and analyze seasonal patterns, enabling better forecasting and strategic planning around peak periods.
    monthly_cycle_trend = df.groupby('month').aggregate({
        'price': 'mean',
        'number_of_reviews': 'sum'
    }).rename(columns={'price': 'avg_price', 'reviews': 'total_reviews'})
    print_analysis_results(monthly_cycle_trend, message='Yealry cycle trend')
    df.to_csv(OUTPUT_PATH)
