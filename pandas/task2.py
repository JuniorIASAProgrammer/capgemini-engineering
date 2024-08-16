import pandas as pd


OUTPUT_PATH = 'pandas/aggregated_airbnb_data.csv'
cleaned_df = pd.read_csv('pandas/cleaned_airbnb_data.csv')


def print_grouped_data(df, message=None, shape=False, info=False):
    if message:
        print(f"----------------{message}---------------")
    if shape:
        print(f"Shape - {df.shape}")
    if info:
        print(f"Info:")
        df.info()
    print(df.head(), end="\n\n")


if __name__ == '__main__':
    print("1.Data Selection and Filtering")
    # Use .iloc and .loc to select specific rows and columns based on both position and labels.
    print_grouped_data(cleaned_df.loc[0:3, ['name', 'neighbourhood_group']], message='.loc usage')
    print_grouped_data(cleaned_df.iloc[0:3, 4:6], message='.iloc usage')
    #Filter the dataset to include only listings in specific neighborhoods (e.g., Manhattan, Brooklyn)
    two_neighbour_cleaned_df = cleaned_df[cleaned_df['neighbourhood'].isin(['Kensington', 'Harlem'])]
    print_grouped_data(two_neighbour_cleaned_df, message='price greater than $100 and a number_of_reviews greater than 10')
    #Further filter the dataset to include only listings with a price greater than $100 and a number_of_reviews greater than 10.
    cleaned_df_filtered = cleaned_df[(cleaned_df['price'] > 100) & (cleaned_df['number_of_reviews'] > 10)]
    #Select columns of interest such as neighbourhood_group, price, minimum_nights, number_of_reviews, price_category and availability_365 for further analysis.
    cleaned_df_subset = cleaned_df_filtered[['neighbourhood_group', 'price', 'minimum_nights', 'number_of_reviews', 'price_category', 'availability_365']]
    print_grouped_data(cleaned_df_subset, message='Selected columns')
    
    print("2.Aggregation and Grouping")
    grouped_cleaned_df = cleaned_df.groupby(['neighbourhood_group', 'price_category'])
    #Calculate the average price and minimum_nights for each group.
    avg_price_and_min_nights = grouped_cleaned_df.aggregate(
        mean_price=('price', 'mean'), 
        mean_minimum_nights=('minimum_nights', 'mean')
    ).sort_values(['neighbourhood_group', 'mean_price'], ascending=[True, False])
    print_grouped_data(avg_price_and_min_nights, message='Avg price and minimum_nights')
    #Compute the average number_of_reviews and availability_365 for each group to understand typical review counts and availability within each neighborhood and price category.
    avg_number_of_reviews_and_availability_365 = grouped_cleaned_df.aggregate(
        average_number_of_reviews=('number_of_reviews', 'mean'), 
        average_availability_365=('availability_365', 'mean')
    ).sort_values(['neighbourhood_group'], ascending=[True])
    print_grouped_data(avg_number_of_reviews_and_availability_365, message='Avg number_of_reviews and availability_365')
    
    print("3.Data Sorting and Ranking")
    #Sort the data by price in descending order and by number_of_reviews in ascending order.
    price_desc_reviews_asc = cleaned_df.sort_values(by=['price', 'number_of_reviews'], axis=0, ascending=[False, True])
    print_grouped_data(price_desc_reviews_asc, message='Price desc reviews asc')
    #Create a ranking of neighborhoods based on the total number of listings and the average price.
    neighborhood_stats = cleaned_df.groupby('neighbourhood').aggregate(
        total_listings=('price', 'size'),
        avg_price=('price', 'mean')
    ).reset_index()
    rank_total_listing = neighborhood_stats.sort_values(by=['total_listings'], ascending=[False])
    print_grouped_data(rank_total_listing, message='Rank by total_listing')
    rank_avg_price = neighborhood_stats.sort_values(by=['avg_price'], ascending=[False])
    print_grouped_data(rank_avg_price, message='Rank by avg price')

    full_grouped = grouped_cleaned_df.aggregate(
        mean_price=('price', 'mean'), 
        mean_minimum_nights=('minimum_nights', 'mean'),
        average_number_of_reviews=('number_of_reviews', 'mean'), 
        average_availability_365=('availability_365', 'mean')
    )
    full_grouped.to_csv(OUTPUT_PATH)
