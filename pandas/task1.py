import pandas as pd


DATASET_PATH = "pandas/AB_NYC_2019.csv"
CLEANED_DATASET_PATH = 'pandas/cleaned_airbnb_data.csv'


def categorize_price(price):
    if price < 100:
        return 'Low'
    elif 100 <= price < 300:
        return 'Medium'
    else:
        return 'High'
    
def categorize_minimum_nights(n_days):
    if n_days <= 3:
        return 'short-term'
    elif 4 <= n_days <= 14:
        return 'medium-term'
    else:
        return 'long-term'
    
def print_dataframe_info(df, message=None, shape=False, info=False):
    if message:
        print(f"----------------{message}---------------")
    if shape:
        print(f"Shape - {df.shape}")
    if info:
        print(f"Info:")
        df.info()
    print("Dataframe head")
    print(df.head(), end="\n\n")


if __name__ == '__main__':
    df = pd.read_csv(DATASET_PATH)
    assert type(df) == pd.DataFrame 
    assert df.shape == (48895, 16), 'Unexpected shape of dataframe'
    print_dataframe_info(df, message='Raw dataset', shape=True, info=True)

    # 2
    n_nan_values = df.isna().sum(axis=0)
    print_dataframe_info(n_nan_values, message='Number of empty values')
    df.fillna({
        'name': 'Unknown',
        'host_name': 'Unknown',
        'last_review': 'NaT'
    }, inplace=True)

    # 3
    df['price_category'] = df['price'].apply(categorize_price)
    df['length_of_stay_category'] = df['minimum_nights'].apply(categorize_price)
    print_dataframe_info(df, message='Dataset with new price_category and length_of_stay_category', info=True)

    #4 
    number_of_empty_values = df.isna().sum(axis=0)
    print(f"-------------Check result of data cleaning (amount of NaN values)--------------:\n{number_of_empty_values}", end='\n\n')
    price_category_values = df['price_category'].value_counts()
    print(f"-------------New price_category column--------------\n{price_category_values}", end='\n\n')
    length_of_stay_category_values = df['length_of_stay_category'].value_counts()
    print(f"-------------New length_of_stay_category column---------------\n{length_of_stay_category_values}", end='\n\n')

    rows_price_zero_or_less = df[df['price'] <= 0]
    print_dataframe_info(rows_price_zero_or_less, message='Subset of rows with non-positive price', shape=True)

    df = df[df['price'] > 0]
    print_dataframe_info(df, message='Dataset with filtered price', shape=True)

    df.to_csv(CLEANED_DATASET_PATH, index=False)

    # 6
    assert all(column in df.columns for column in ['price_category', 'length_of_stay_category']), "Missing columns"
    assert categorize_price(50) == 'Low', 'Wrong price categorization for param 50'
    assert categorize_price(100) == 'Medium', 'Wrong price categorization for param 100'
    assert categorize_price(500) == 'High', 'Wrong price categorization for param 500'
    assert categorize_minimum_nights(1) == 'short-term'
    assert categorize_minimum_nights(5) == 'medium-term'
    assert categorize_minimum_nights(30) == 'long-term'
    assert (df['price'] < 0).sum() == 0, "Negative values in 'price' column"
    assert all(df[['name', 'host_name', 'last_review']].isna().sum(axis=0) == 0), "Important columns ('name', 'host_name', 'last_review') have missing values"
    assert pd.read_csv(CLEANED_DATASET_PATH).shape == (48884, 18), 'Cleaned dataset has unexpected shape'
