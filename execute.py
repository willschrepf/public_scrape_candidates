import requests
import pandas as pd
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Function to search Google using scrape-it.cloud API
def search_google(search_term, api_key):
    url = "https://api.scrape-it.cloud/scrape/google/serp"
    headers = {"x-api-key": api_key}
    params = {"q": search_term}
    response = requests.get(url, headers=headers, params=params)
    return response.json() if response.status_code == 200 else None

# Function to extract LinkedIn URLs
def get_linkedin_urls(search_query, api_key):
    results = search_google(search_query, api_key)
    if results and 'organicResults' in results:
        return [res['link'] for res in results['organicResults'] if 'linkedin.com/in/' in res['link']]
    return []

# Function to update a single row with LinkedIn URLs
def update_row_with_linkedin_urls(row, api_key):
    base_query = f"{row['Name']} {row['Office']} {row['State']}"
    queries = {
        'cand_url': f"{base_query} linkedin",
        'cm_url': f"{base_query} campaign manager linkedin",
        'fd_url': f"{base_query} finance director linkedin"
    }

    # Dictionary to hold the result URLs
    linkedin_urls = {}

    # Track seen URLs to avoid duplicates
    seen_urls = set()

    # Execute the searches
    for key, query in queries.items():
        urls = get_linkedin_urls(query, api_key)
        for url in urls:
            if url not in seen_urls:
                linkedin_urls[key] = url
                seen_urls.add(url)
                break

    return linkedin_urls

# Main function to process the DataFrame
def process_dataframe(df):
    api_key = os.getenv('API_KEY')
    with ThreadPoolExecutor(max_workers=80) as executor:
        future_to_index = {executor.submit(update_row_with_linkedin_urls, row, api_key): index for index, row in df.iterrows()}

        # As each future completes, update the DataFrame
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                linkedin_urls = future.result()
                for key, url in linkedin_urls.items():
                    df.at[index, key] = url
                print(f"Successfully processed row {index}.")
            except Exception as e:
                print(f"Row {index} generated an exception: {e}")

    return df

df = pd.read_csv('dems.csv')

df_updated = process_dataframe(df)

df_updated.to_csv('updated_dems_415.csv', index=False)