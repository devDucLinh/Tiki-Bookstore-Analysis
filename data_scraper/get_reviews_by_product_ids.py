# Import libraries
import time 
import requests
import pandas as pd
import os
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Set directory paths
DATA_DIR = '/content/drive/MyDrive/data'  
LOG_DIR = '/content/drive/MyDrive/data/logs'  

# Set file names
INPUT_FILE = 'nha-sach-tiki.csv'
OUTPUT_FILE = INPUT_FILE.replace('.csv', '-reviews.csv')
LOG_FILE = OUTPUT_FILE.replace('.csv', '.log')

# Set API URL and headers
BASE_URL = "https://tiki.vn/api/v2/reviews"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.51",
    "X-Guest-Token": "DnNSb7co65JF9qgP1fzaWtGmRAwkjvEV" 
}

# Configure logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, LOG_FILE)),
        logging.StreamHandler()
    ]
)

# Define function to retrieve reviews for list of products
def retrieve_reviews(product_data):  
    """Retrieves reviews for a list of products."""
    review_data = pd.DataFrame() # Initialize reviews dataframe
    total_products = len(product_data) # Get total number of products
    
    # Use ThreadPoolExecutor for concurrent execution 
    with ThreadPoolExecutor() as executor:  
        # Use tqdm to display progress bar
        results = list(tqdm(executor.map(get_reviews_for_product, product_data.iterrows()), total=total_products))  
        # Concatenate results into single dataframe
        review_data = pd.concat(results, ignore_index=True)
    return review_data  

# Define function to retrieve reviews for a single product
def get_reviews_for_product(row):
    """Retrieves reviews for a specific product."""
    index, product = row # Unpack row tuple
    
    # Set API request parameters
    params = {
        "limit": 20,
        "include": "comments,contribute_info,attribute_vote_summary",
        "sort": "score|desc,id|desc,stars|all",
        "spid": product['seller_product_id'],
        "product_id": product['id'],
        "seller_id": product['seller_id']
    }
    review_data = pd.DataFrame() # Initialize reviews dataframe
    page = 1 # Initialize page number
    att = 0 # Initialize attempt counter

    while True:
        params['page'] = page # Set page number parameter
        response = requests.get(BASE_URL, params=params, headers=HEADERS) # Make API request
        
        # If response is successful, extract reviews and go to next page
        if response.status_code == 200:  
            reviews = response.json()  
            total_pages = reviews['paging']['last_page']  
            temp = pd.DataFrame(reviews['data'])  
            review_data = pd.concat([review_data, temp], ignore_index=True)  
            if page >= total_pages:  
                break  
            page += 1  
        
        # If response fails, wait 10s and try again up to 5 times 
        else: 
            att += 1  
            if att == 5:  
                logging.error(f"Max attempts reached: Error: {response.status_code}")  
                break  
            logging.error(f"Error: {response.status_code}. Trying again in 10s.")  
            time.sleep(10)  
            continue 
    return review_data  

if __name__ == "__main__":  
    
    # Read product data from CSV
    product_data = pd.read_csv(os.path.join(DATA_DIR, INPUT_FILE), dtype='str')  
    
    # Drop duplicate products based on ID and seller info
    product_data.drop_duplicates(subset=['id', 'seller_id', 'seller_product_id'], inplace=True)  
    
    # Convert review count column to integers
    product_data['review_count'] = product_data['review_count'].astype(int)
    
    # Filter for products with reviews
    product_data = product_data[product_data['review_count'] > 0]  
    
    # Inform user of number of products to crawl
    logging.info(f"Crawling reviews for {len(product_data)} products...")
    
    # Retrieve reviews for all products
    reviews_combined = retrieve_reviews(product_data)  
    
    # Inform user that crawling is complete
    logging.info("Reviews processing complete.")
    
    # Save reviews data to CSV
    reviews_combined.to_csv(os.path.join(DATA_DIR, OUTPUT_FILE), index=False)