import os
import requests
import logging
import time
import pandas as pd
from tqdm import tqdm
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.51",
    "X-Guest-Token": "DnNSb7co65JF9qgP1fzaWtGmRAwkjvEV"
}

# Set up data and log directories
data_directory = './data'
log_directory = './data/logs'
os.makedirs(log_directory, exist_ok=True)


def fetch_data(category_id, min_price, page, retry_delay=5, max_retries=3):
    """
    Fetches data from the Tiki API for a specific category.

    Args:
        category_id (str): The ID of the category.
        min_price (int): The minimum price of the products.
        page (int): The page number of the results.
        retry_delay (int): The delay between retries in seconds.
        max_retries (int): The maximum number of retries.

    Returns:
        tuple: A tuple containing the price of the last item and the number of items fetched.
    """

    base_url = "https://tiki.vn/api/personalish/v1/blocks/listings"
    limit = 100
    sort = 'price,asc'
    total_items = 0

    params = {
        'limit': limit,
        'aggregations': 2,
        'category': category_id,
        'page': page,
        'sort': sort,
        'price': min_price
    }

    retry_count = 0
    while retry_count < max_retries:
        response = requests.get(base_url, headers=HEADERS, params=params)

        if response.status_code == 200:
            data = response.json()['data']
            products = pd.DataFrame(data)
            item_count = len(products)

            if item_count > 0:
                # Get the price of the last item
                last_item_price = products.tail(1)['price']

                # Log the price of the last item
                logging.info(f"Price of the last item: {last_item_price}")

                # Append the products to the data file
                products.to_csv(data_file, mode='a', header=False, index=False)

                # Update the total item count
                total_items += item_count

                return last_item_price, item_count

        # Retry after a delay if the request is denied
        if response.status_code == 403:
            retry_count += 1
            if retry_count < max_retries:
                logging.warning(f"Request denied. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
        else:
            break

    return None, 0


def get_subcategories(category_id, retry_delay=5, max_retries=3):
    """
    Get subcategories for a given category ID.

    Args:
        category_id (str): The ID of the category.
        retry_delay (int): The delay between retries in seconds.
        max_retries (int): The maximum number of retries.

    Returns:
        pandas.DataFrame: DataFrame containing the subcategories.
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get(
                'https://tiki.vn/api/personalish/v1/blocks/listings',
                headers=HEADERS,
                params={'category': category_id, 'aggregations': 2}
            )
            response.raise_for_status()
            json_data = response.json()

            if pd.DataFrame(json_data['filters'][0])['query_name'][0] == 'category':
                subcategories = pd.DataFrame(json_data['filters'][0]['values'])
                return subcategories
            else:
                logging.warning(f"Unable to fetch subcategories for category {category_id}.")
                return pd.DataFrame()
        except Exception as e:
            logging.error(f"Error occurred while fetching subcategories for category {category_id}. Error: {str(e)}")

            retry_count += 1
            if retry_count < max_retries:
                logging.info(f"Retrying after {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.warning(f"Max attempts reached. Unable to fetch subcategories for category {category_id}.")

    return pd.DataFrame()


def crawl_category(category_id):
    """
    Crawls the Tiki website for a specific category.

    Args:
        category_id (str): The ID of the category.

    Returns:
        None
    """

    min_price = 0
    total_fetched_items = 0
    page = 1


    with tqdm(unit="item", ncols=80, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
        while True:
            last_item_price, fetched_items_count = fetch_data(category_id, min_price, page)

            if last_item_price is None:
                break

            total_fetched_items += fetched_items_count

            if total_fetched_items >= 2000:
                min_price = last_item_price
                total_fetched_items = 0
                page = 1
            elif fetched_items_count == 100:
                page += 1
            else:
                break

            pbar.update(fetched_items_count)


def crawl_combined_subcategory(category_id):
    """
    Crawls the Tiki website for products by combining all subcategories of a specific category.

    Args:
        category_id (int): The ID of the category.

    Returns:
        None
    """
    subcategories = get_subcategories(category_id)
    if subcategories.empty:
        crawl_category(category_id)
    else:
        with tqdm(total=len(subcategories), ncols=80, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}', desc="Subcategories") as pbar:
            for _, subcategory in subcategories.iterrows():
                subcategory_id = subcategory['query_value']
                crawl_combined_subcategory(subcategory_id)
                pbar.update(1)


if __name__ == '__main__':
    # Prompt user to enter category link
    category_link = input('Enter category link: ')

    # Extract category name from the link
    category_name = category_link.split('/')[-2]

    # Extract category ID from the link
    category_id = int(category_link.split("/")[-1].split("c")[1])

    # Create data file path
    data_file = os.path.join(data_directory, f'{category_name}.csv')

    # Set up logging
    log_file = os.path.join(log_directory, f'{category_name}.log')
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Crawl the subcategories
    crawl_combined_subcategory(category_id)
