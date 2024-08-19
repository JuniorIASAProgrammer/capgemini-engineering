import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


df = pd.read_csv('data/NY_Airbnb/cleaned_airbnb_data.csv')


def plot_bar_of_listing_by_neighbourhood_group(df):
    plt.figure(figsize=(11,6))
    bars = plt.barh(df.index, df.values)

    for bar in bars:
        plt.text(bar.get_width(), bar.get_y() + bar.get_height()/2, int(bar.get_width()), ha='left', va='center')

    plt.title('Distribution of Listings Across Neighbourhood Groups', fontsize=14)
    plt.xlabel('Number of Listings')
    plt.ylabel('Neighbourhood Group')
    plt.tight_layout()
    plt.show()


def box_plot_of_price_across_neighbourhood(df, groups):
    plt.figure(figsize=(12, 6))
    box = plt.boxplot(df, patch_artist=True, notch=False, vert=True, tick_labels=groups)
    colors = plt.cm.Paired.colors
    for patch, color in zip(box['boxes'], colors):
        patch.set_facecolor(color)
    plt.title('Distribution of Prices across Neighbourhood Groups', fontsize=16)
    plt.xlabel('Neighbourhood Group', fontsize=10)
    plt.ylabel('Price (Log Scale)', fontsize=10)
    plt.yscale('log')
    plt.tight_layout()
    plt.show()


def grouped_bar_availability_for_room_type(df):
    df['mean_availability'].plot(kind='bar', yerr=df['std_availability'], capsize=4, figsize=(12, 8), error_kw=dict(ecolor='black', lw=1.5))
    plt.title('Average Availability 365 Days by Room Type and Neighbourhood', fontsize=16)
    plt.xlabel('Neighbourhood', fontsize=14)
    plt.ylabel('Average Availability (365 days)', fontsize=14)
    plt.legend(title='Room Type', fontsize=12)
    plt.xticks(rotation=0)
    plt.tight_layout(pad=2)
    plt.show()


def scatter_price_to_number_reviews(df):
    plt.figure(figsize=(9,6))
    sns.regplot(data=df, x='price', y='number_of_reviews', color='red')
    sns.scatterplot(data=df, x='price', y='number_of_reviews', hue='room_type', sizes=0.5)
    plt.title('Average Availability 365 Days by Room Type and Neighbourhood', fontsize=14)
    plt.xlabel('Price')
    plt.ylabel('Number of reviews')
    plt.show()


def line_plot_review_over_time(df):
    plt.figure(figsize=(12, 8))
    sns.lineplot(data=df, x='year_month', y='rolling_reviews', hue='neighbourhood_group')
    plt.title('Trend of Number of Reviews Over Time by Neighbourhood Group', fontsize=16)
    plt.xlabel('Date of Last Review', fontsize=14)
    plt.ylabel('Number of Reviews (Rolling Average)', fontsize=14)
    plt.yscale('log')
    plt.legend(title='Neighbourhood Group', fontsize=12)
    plt.tight_layout()
    plt.show()


def stacked_bar_reviews_to_room_type(df):
    df.plot(kind='bar', stacked=True, figsize=(10, 7))
    plt.title('Number of Reviews by Room Type and Neighbourhood Group')
    plt.xlabel('Neighbourhood Group')
    plt.xticks(rotation=0)
    plt.ylabel('Number of Reviews')
    plt.legend(title='Room Type')
    plt.show()


if __name__ == '__main__':
    #1
    listing_counts = df['neighbourhood_group'].value_counts().sort_values(ascending=True)
    plot_bar_of_listing_by_neighbourhood_group(listing_counts)

    #2
    neighbourhood_groups = df['neighbourhood_group'].unique()
    prices_by_group = [df[df['neighbourhood_group'] == group]['price'].values for group in neighbourhood_groups]
    box_plot_of_price_across_neighbourhood(prices_by_group, neighbourhood_groups)

    #3 
    grouped = df.groupby(['neighbourhood_group', 'room_type']).agg(
        mean_availability=('availability_365', 'mean'),
        std_availability=('availability_365', 'std')
    ).reset_index()
    pivot_df = grouped.pivot(index='neighbourhood_group', columns='room_type', values=['mean_availability', 'std_availability'])
    grouped_bar_availability_for_room_type(pivot_df)

    #4
    scatter_price_to_number_reviews(df)

    #5
    df['last_review'] = pd.to_datetime(df['last_review'])
    df['year_month'] = df['last_review'].dt.to_period('M')
    monthly_grouped = df.groupby(['year_month', 'neighbourhood_group']).agg({'number_of_reviews': 'sum'}).reset_index()
    monthly_grouped['year_month'] = monthly_grouped['year_month'].dt.to_timestamp()
    monthly_grouped['rolling_reviews'] = monthly_grouped.groupby('neighbourhood_group')['number_of_reviews'].transform(lambda x: x.rolling(window=2, min_periods=1).mean())
    line_plot_review_over_time(monthly_grouped)

    #6
    corr_df = df.groupby('neighbourhood_group').apply(lambda x: x[['price', 'availability_365']].corr().iloc[0, 1]).reset_index()
    corr_df.columns = ['neighbourhood_group', 'correlation']
    #print(corr_df)

    #7
    pivot_df = df.pivot_table(index='neighbourhood_group', columns='room_type', values='number_of_reviews', aggfunc='sum', fill_value=0)
    stacked_bar_reviews_to_room_type(pivot_df)
