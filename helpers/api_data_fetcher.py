import requests
import pandas as pd 

EXAMPLE_API_VAL = {
      "validator_address": "0x95373bcf8e2c64e1c373a6e534c002f210adbcc84c5abda3b6306677e171430ae50781a51c9f579a47622e334dba2412",
      "validator_index": "1",
      "state": "active_ongoing",
      "activated_at": "2023-01-14T01:13:59Z",
      "activated_epoch": 174049,
      "delegated_at": "2023-01-14T01:13:59Z",
      "delegated_block": 16397387,
      "exited_at": "2023-01-14T01:13:59Z",
      "exited_epoch": 174049,
      "deposit_tx_sender": "0xe1f4acc0affB36a805474e3b6ab786738C6900A2",
      "execution_fee_recipient": "0xe1f4acc0affB36a805474e3b6ab786738C6900A2",
      "withdrawal_credentials": "010000000000000000000000e1f4acc0affb36a805474e3b6ab786738c6900a2",
      "effective_balance": "32000000000000000000",
      "balance": "32076187808000000000",
      "consensus_rewards": "76187808000000000",
      "execution_rewards": "0",
      "rewards": "76187808000000000",
      "claimable_execution_rewards": "76187808000000000",
      "claimable_consensus_rewards": "76187808000000000",
      "gross_apy": 3.407,
      "is_kiln": True,
      "updated_at": "2023-01-14T01:13:59Z",
      "eigenlayer": {
        "can_restake": True,
        "is_restaked": True,
        "points": 16287.724444444444
      },
      "estimated_next_skimming_slot": 16397387,
      "estimated_next_skimming_at": "2023-01-14T01:13:59Z"
    }


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
    df = pd.DataFrame(columns=EXAMPLE_API_VAL.keys())
    df = pd.concat([df, pd.DataFrame(payload["data"])]) 
    for page in range(2, total_pages+1): 
        response = requests.get(api_url, params={"scope": params["scope"], "current_page": page, "page_size": params["page_size"]}, headers=headers)
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
    return df
    

def fetch_all_non_kiln_validators(api_url, params, headers, output_csv, total_pages):
    payload = fetch_page(api_url, params, headers)
    df = pd.DataFrame(columns=EXAMPLE_API_VAL.keys())
    df = pd.concat([df, pd.DataFrame(payload["data"])]) 
    for page in range(2, total_pages+1): 
        print(f"Fetching page {page} of {total_pages}")
        response = requests.get(api_url, params={"scope": params["scope"], "current_page": page, "page_size": params["page_size"]}, headers=headers)
        if response.status_code == 200:
            res = response.json()
            page_df = pd.DataFrame(res["data"])
            df = pd.concat([df, page_df], ignore_index=True)
            if len(page_df) < params["page_size"]:
               break 
        else:
            print(f" {response.status_code} Error: Failed to fetch data from {api_url}?current_page={page}")
            break 
    df = df[df["is_kiln"] == False]
    df.to_csv(output_csv, index=False)
    print(f"Data fetched from {api_url} and saved to {output_csv}")
    return df


# Function to calculate APYs
def calculate_apys(validators):
    df = pd.DataFrame(validators)
    
    df["effective_balance_eth"] = df["effective_balance"].astype(float) / 1e18
    df["consensus_rewards_eth"] = df["consensus_rewards"].astype(float) / 1e18
    df["execution_rewards_eth"] = df["execution_rewards"].astype(float) / 1e18

    # Calculate total rewards in ETH
    df["total_rewards_eth"] = df["consensus_rewards_eth"] + df["execution_rewards_eth"]

    # Calculate CL APY and EL APY
    df["cl_apy"] = df["gross_apy"] * (df["consensus_rewards_eth"] / df["total_rewards_eth"])
    df["el_apy"] = df["gross_apy"] * (df["execution_rewards_eth"] / df["total_rewards_eth"])
  
    return df

def calculate_overall_apys(df):
    return {
        'avg_gross_apy': df['gross_apy'].mean(),
        'avg_cl_apy': df['cl_apy'].mean(),
        'avg_el_apy': df['el_apy'].mean() 
    }
    
def get_max_apy_validators(df, group_name):
    max_gross_apy = df.loc[df['gross_apy'].idxmax()]
    max_cl_apy = df.loc[df['cl_apy'].idxmax()]
    max_el_apy = df.loc[df['el_apy'].idxmax()]
    
    print(f"\n{group_name} Validators with Maximum APYs:")
    print("\nMax Gross APY Validator:")
    print(f"Validator Address: {max_gross_apy['validator_address']}")
    print(f"Gross APY: {max_gross_apy['gross_apy']:.4f}%")
    print(f"CL APY: {max_gross_apy['cl_apy']:.4f}%")
    print(f"EL APY: {max_gross_apy['el_apy']:.4f}%")
    
    print("\nMax CL APY Validator:")
    print(f"Validator Address: {max_cl_apy['validator_address']}")
    print(f"Gross APY: {max_cl_apy['gross_apy']:.4f}%")
    print(f"CL APY: {max_cl_apy['cl_apy']:.4f}%")
    print(f"EL APY: {max_cl_apy['el_apy']:.4f}%")
    
    print("\nMax EL APY Validator:")
    print(f"Validator Address: {max_el_apy['validator_address']}")
    print(f"Gross APY: {max_el_apy['gross_apy']:.4f}%")
    print(f"CL APY: {max_el_apy['cl_apy']:.4f}%")
    print(f"EL APY: {max_el_apy['el_apy']:.4f}%")

    return { 
        'max_gross_apy_validator': max_gross_apy,
        'max_cl_apy_address': max_cl_apy,
        'max_el_apy_address': max_el_apy
    }