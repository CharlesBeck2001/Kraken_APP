#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 00:16:32 2024

@author: charlesbeck
"""

import streamlit as st
import pandas as pd
import krakenex
#import mysql.connector
from datetime import datetime, timedelta
import time
import requests
#import matplotlib.pyplot as plt
import numpy as np
import pytz

st.set_page_config(
    page_title='Kraken Trades Dashboard'
    )

# Initialize the Kraken API
api = krakenex.API()

# Set API credentials
api.key = "nx4ha0y6Ym+5nUVBzagNNcoGbrUPQnihv1HpYtjIiQCKO+YJKyKp2eRc"  # Replace with your Kraken API Key
api.secret = "/n/unia48BxTUR8W7TgtOARORj3t0Q9evJKCeJ5gxK3bBE+CCGB0GVMi0+v/Hq/2w3KjboymD9dgujl5DQoTCw=="  # Replace with your Kraken Secret Key


# Fetch all tradeable pairs
def get_all_pairs():
    """
    Fetch all available trading pairs on Kraken.
    """
    try:
        response = api.query_public('AssetPairs')
        if 'error' in response and response['error']:
            print(f"Error fetching pairs: {response['error']}")
            return []
        return response['result'].keys()
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
def fetch_trade_data(pair, since=None):
    """
    Fetch trade data for a given trading pair.
    :param pair: Trading pair (e.g., XBTUSD)
    :param since: Optional timestamp to fetch data from a specific time.
    :return: List of trades with timestamp, volume, price, and base unit.
    """
    try:
        # Fetch tradable asset pairs to determine the base currency
        asset_pairs_response = api.query_public('AssetPairs')
        if 'error' in asset_pairs_response and asset_pairs_response['error']:
            print(f"Error fetching asset pairs: {asset_pairs_response['error']}")
            return []

        asset_pairs = asset_pairs_response['result']

        # Extract the base currency for the given pair
        if pair in asset_pairs:
            base_unit = asset_pairs[pair]['base']
        else:
            print(f"Pair {pair} not found in tradable asset pairs.")
            return []

        # Fetch trade data
        response = api.query_public('Trades', {'pair': pair, 'since': since})
        if 'error' in response and response['error']:
            print(f"Error fetching trades for {pair}: {response['error']}")
            return []

        trades = response['result'][pair]
        
        # Add base unit information to each trade
        enhanced_trades = []
        for trade in trades:
            enhanced_trade = {
                'price': trade[0],  # Price in quote currency
                'volume': trade[1],  # Volume in base currency
                'timestamp': trade[2],  # Timestamp
                'base_unit': base_unit  # Base currency
            }
            enhanced_trades.append(enhanced_trade)

        return enhanced_trades

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
def fetch_all_trade_data(pair, days_ago):
    """
    Fetch all trade data for a given trading pair starting from `days_ago` until now.
    :param pair: Trading pair (e.g., XBTUSD).
    :param days_ago: Number of days ago to start fetching data.
    :return: List of trades with price, volume, timestamp, and base unit.
    """
    try:
        # Fetch tradable asset pairs to determine the base currency
        asset_pairs_response = api.query_public('AssetPairs')
        if 'error' in asset_pairs_response and asset_pairs_response['error']:
            print(f"Error fetching asset pairs: {asset_pairs_response['error']}")
            return []

        asset_pairs = asset_pairs_response['result']

        # Extract the base currency for the given pair
        if pair in asset_pairs:
            base_unit = asset_pairs[pair]['base']
        else:
            print(f"Pair {pair} not found in tradable asset pairs.")
            return []

        utc_timezone = pytz.utc
        
        # Calculate the starting timestamp for `days_ago`
        start_time = datetime.now(utc_timezone) - timedelta(days=days_ago)
        since = int(time.mktime(start_time.timetuple()) * 1_000_000_000) # Convert to nanoseconds

        all_trades = []
        
        # Loop to fetch paginated results
        while True:
            response = api.query_public('Trades', {'pair': pair, 'since': since})
            if 'error' in response and response['error']:
                print(f"Error fetching trades for {pair}: {response['error']}")
                break

            trades = response['result'].get(pair, [])
            if not trades:
                break

            # Add base unit information to each trade
            for trade in trades:
                enhanced_trade = {
                    'price': trade[0],  # Price in quote currency
                    'volume': trade[1],  # Volume in base currency
                    'timestamp': trade[2],  # Timestamp
                    'base_unit': base_unit  # Base currency
                }
                all_trades.append(enhanced_trade)

            # Update the `since` parameter to the last trade's timestamp
            last_trade_time = trades[-1][2]  # Timestamp of the last trade in seconds
            since = int(last_trade_time * 1_000_000_000)  # Convert to nanoseconds

            # If less than 1,000 trades were returned, we're at the end
            if len(trades) < 1_000:
                break

        return all_trades

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
def calculate_total_volume_in_target_currency(pair, trade):
    """
    Calculate the total trade volume in the target currency (USDT, USD, or USDC)
    based on the pair and trade data.
    """
    price = float(trade['price'])
    volume = float(trade['volume'])
    
    # Check if the pair's quote currency is USDT, USD, or USDC
    if 'USDT' in pair or 'USD' in pair or 'USDC' in pair:
        if pair.endswith('USDT') or pair.endswith('USD') or pair.endswith('USDC'):
            # If the base currency is USDT, USD, or USDC, we need to multiply by the price to get volume in base currency
            total_volume_in_target_currency = volume * price  # volume (in base) * price (in quote) gives total in base currency
        else:
            # If the pair is like 'USDTADA' or 'USDADA' where base currency is USDT/USD/USDC
            total_volume_in_target_currency = volume / price  # volume (in quote) / price (in base) gives total in base currency
    else:
        # If the pair is not involving USDT, USD, or USDC, perform conversion using an external source
        return(0)
    
    return total_volume_in_target_currency

def process_trades_with_volume_in_usd(df):
    """
    Given a DataFrame of trades, calculate the total volume in USD, USDT, or USDC for each trade.
    :param df: DataFrame with trades. Columns: pair, price, volume, timestamp, base_unit.
    :return: DataFrame with an additional column `total_volume_in_usd`.
    """
    # Create a new column to store the total volume in USD, USDT, or USDC
    df['total_volume_in_usd'] = df.apply(lambda row: calculate_total_volume_in_target_currency(row['pair'], row), axis=1)
    
    return df
    
#ile_path = 'coingecko_coins.csv'  # Adjust the path if necessary
#coins_df = pd.read_csv('/Users/charlesbeck/coingecko_coins.csv')


pairs = get_all_pairs()  # Get all tradeable pairs



pair_vals = []
for pair in pairs:
    pair_vals.append(pair)

pair_list = list(pair_vals)

filtered_pairs = [pair for pair in pair_list if pair.endswith('USD') or pair.endswith('USDT') or pair.startswith('USD')]
'''
Kraken Data Dashboard

Browse Kraken data by pair sampled from the last fourteen days of trading.

'''

selected_pairs = st.multiselect('Which pairs would you like to view?', filtered_pairs, ['MKRUSD', 'YGGUSD', 'XLTCZUSD'])

''
''
''
aggregated_data = []

for pair in selected_pairs:
    
    trades = fetch_all_trade_data(pair, 14)
    if len(trades) > 0:
        
        trades_df = pd.DataFrame(trades)
        trades_df["pair"] = pair
        df_trades_with_volume_in_usd = process_trades_with_volume_in_usd(trades_df)
        #trades_df["volume"] =  trades_df["volume"].astype(float)
        #trades_df["usd_volume"] = trades_df["volume"] * price
        trades_df = trades_df.sort_values("total_volume_in_usd")
        total_volume = float(trades_df["total_volume_in_usd"].sum())
        trades_df["cumulative_percentage"] = trades_df["total_volume_in_usd"].cumsum() / total_volume
    
        trades_df["log10_usd_volume"] = np.log10(trades_df["total_volume_in_usd"])
        trades_df = trades_df[trades_df['log10_usd_volume'] >= 0]
    
        if len(trades_df) > 5000:
        
            trades_df = trades_df.sample(1000, random_state=42).sort_values("log10_usd_volume")
    
        
        aggregated_data.append(trades_df[["log10_usd_volume", "cumulative_percentage", "pair"]])

combined_df = pd.concat(aggregated_data)

st.line_chart(combined_df, x='log10_usd_volume', y='cumulative_percentage', color='pair')
