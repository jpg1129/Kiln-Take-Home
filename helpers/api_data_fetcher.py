import requests
import pandas as pd 


def fetch_page(api_url, params, headers): 
    response = requests.get(api_url, params=params, headers=headers)
    if response.status_code == 200:
        payload= response.json()
        return payload
    else:
        print(f" {response.status_code} Error: Failed to fetch data from {api_url}")
    return None

def fetch_all_pages(api_url, params, headers, output_csv): 
    payload = fetch_page(api_url, params, headers)
    total_pages = payload["pagination"].get("total_pages", 0)
    page_size = payload["pagination"].get("page_size", 0)
    if total_pages == 0 or page_size == 0:
        print(f"Error: Failed to fetch data from {api_url}")
        return 
    df = pd.DataFrame(payload["data"])
    for page in range(2, 3): 
        response = requests.get(api_url, params={"scope": "kiln", "current_page": page, "page_size": 100}, headers=headers)
        if response.status_code == 200:
            res = response.json()
            page_df = pd.DataFrame(res["data"])
            df = pd.concat([df, page_df], ignore_index=True)
            if len(page_df) < int(page_size):
               break 
        else:
            print(f" {response.status_code} Error: Failed to fetch data from {api_url}?current_page={page}")
            break 
    df.to_csv(output_csv, index=False)
    print(f"Data fetched from {api_url} and saved to {output_csv}")
    

