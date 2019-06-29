import os
import time
import requests
import pandas as pd
from datetime import datetime

def nixtime_to_date(timestamp):
    date = datetime.fromtimestamp(int(timestamp))
    date = date.strftime('%Y-%m-%d')
    return date

def reorder(srcdir, dstdir):
    for fp in os.listdir(srcdir):
        new_fp = os.path.join(dstdir, fp) 
        try:
            exchange = pd.read_csv(os.path.join(srcdir, fp))
        except pd.errors.ParserError as e:
            print('Skipping {} due to parser error.'.format(
                os.path.join(srcdir, fp)))
            continue
        exchange['Date'] = pd.to_datetime(exchange.Date)
        exchange = exchange.sort_values(by='Date', ascending=0)
        exchange.to_csv(new_fp, index=False)

def relabel(df, affix, sep='_', pre=True):
    """
    Add an affix to column names in the DataFrame using the desired separator
    (default: '_'). This is to ensure that when dataframes are merged, columns
    will have unique names. 
    If `pre` is true, the affix is a prefix, otherwise, it's a postfix. 
    """
    #make these look a little nicer, and rename time to Date since it's daily
    d = {'volumefrom':'volume'+sep+'from', 
         'volumeto':'volume'+sep+'to', 
         'time':'Date'} 
    df.rename(columns=d, inplace=True)
    #we won't affix the date, since it should be unique even after a merge
    cols = list(df)
    cols.remove('Date') 
    #rename the rest of the columns by affixing them with the desired affix
    if pre:
        mapping = {col:affix+sep+col for col in cols}
    else:
        mapping = {col:col+sep+affix for col in cols}
    df.rename(columns=mapping, inplace=True)
    return df

def download_cryptocompare_exchanges():
    """
    Download a dictionary of exchanges available on the cryptocompare.com
    website, with values representing dictionaries of crytpocurrencies to
    a list of the comparison currency they trade into. 
    """
    url = 'https://min-api.cryptocompare.com/data/all/exchanges'
    page = requests.get(url)
    data = page.json()
    return data

def parse_cryptocompare_pairs(cryptocurrencies, currency='USD'):
    """
    Cryptocurrencies is a dictionary of cryptocurrencies to trading currency.
    Parse the dictionary to get a list of cryptocurrencies that can be traded
    into the target currency (default: 'USD').
    """
    ccys = []
    for ccy in cryptocurrencies:
        if currency in cryptocurrencies[ccy]:
            ccys.append(ccy)
    if len(ccys):
        return ccys
    return False

def download_cryptocompare_currencies():
    """
    Download a list of cryptocurrency symbols available on cryptocompare.com. 
    """
    url = 'https://www.cryptocompare.com/api/data/coinlist/'
    page = requests.get(url)
    data = page.json()["Data"]
    symbols = [data[k]["Symbol"] for k in data.keys()]
    return symbols

def dl_cryptocompare_history(symbol, comparison_symbol, 
        all_data=True, limit=1, aggregate=1, exchange=''):
    """
    Download data from the cryptocompare.com website for the target
    cryptocurrency exchange (default: all exchanges). The data will 
    include daily historical prices (in the `comparison_symbol` currency) 
    and volumes to/from. Return data in pandas DataFrame format.
    """
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol, limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    if all_data:
        url += '&allData=true'
    print(url)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    return df

def download_cryptocompare_exchange(exchange, cryptocurrencies, currency="USD"):
    """
    Download historic daily price data for the cryptocurrencies on the target
    exchange, where the prices are listed in the target currency. 
    """
    #the exchange's dataframe will be organized by the Date column
    ex_df = pd.DataFrame(columns=['Date'])
    for ccy in cryptocurrencies:
        ccy = ccy.upper()
        ccy_df = dl_cryptocompare_history(
                ccy, currency, exchange=exchange)
        if ccy_df.empty:
            print('No data for: exchange {}, cryptocurrency {}, target currency {}'.format(
                exchange, ccy, currency))
            time.sleep(1) #wait between downloads so the servers are happy
            continue
        ccy_df = relabel(ccy_df, ccy) #add prefix of cryptocurrency to column
        ex_df = pd.merge(ex_df, ccy_df, on='Date', how='outer')
        time.sleep(1) #wait between downloads so the servers are happy
    
    #put date column first and sort by date
    dates = ex_df['Date']                              #extract date column 
    ex_df.drop(labels=['Date'], axis=1, inplace=True)  #remove from orig df
    ex_df.insert(0, 'Date', dates)                     #insert at the front
    return ex_df

def download_cryptocompare_csvs():
    target_currencies = ['USD', 'BTC']
    #get a list of the available cryptocurrency symbols
    available_cryptocurrencies = set(download_cryptocompare_currencies())
    #get a dictionary of exchanges to the cryptocurrencies they trade
    #the values are dictionaries of currencies to their trading currency
    exchanges = download_cryptocompare_exchanges()

    #we already collected these in a previous run of this script, so skip them
    fps = os.listdir('.')
    done = [result for fp in fps for result in os.path.splitext(fp)]

    #for each exchange, download data for the currencies that are traded
    #into the target currencies on that exchange, and save to csv file
    for exchange in exchanges:
        if exchange in done:
            print("Exchange {} already done; continuing.".format(exchange))
            continue
        exchange_df = pd.DataFrame(columns=['Date']) #we will sort by date
        for currency in target_currencies:
            #get a list of cryptocurrencies that are traded to target currency
            ccys = parse_cryptocompare_pairs(exchanges[exchange])
            if ccys: 
                ccys = set(ccys).intersection(available_cryptocurrencies)
                ccys_on_ex_for_currency = download_cryptocompare_exchange(
                        exchange, ccys, currency)
                #append a label of the target currency to the column names
                ccys_df = relabel(ccys_on_ex_for_currency, currency, pre=False)
                #merge the results for this target currency with the rest
                exchange_df = pd.merge(exchange_df, ccys_df,
                        on='Date', how='outer')

        #create one csv file for each exchange, sorted by lateset date first
        if not exchange_df.empty:
            exchange_df['Date'] = exchange_df['Date'].apply(nixtime_to_date)
            exchange_df['Date'] = pd.to_datetime(exchange_df.Date)
            exchange_df = exchange_df.sort_values(by='Date', ascending=0)
            exchange_df.to_csv(
                    os.path.join('new', exchange+'.csv'), index=False)

def download_kraken_currencies():
    url = 'https://api.kraken.com/0/public/Assets' 
    page = requests.get(url)
    data = page.json()["result"]
    symbols = [data[k] for k in data.keys()]
    url = 'https://api.kraken.com/0/public/AssetPairs' 
    page = requests.get(url)
    data = page.json()["result"]
    pairs = [data[k] for k in data.keys()]
    return (symbols, pairs)

def download_kraken_pair(pair, labels_dohlcv, since=0, interval=1440):
    #url = 'https://api.kraken.com/0/public/Trades?pair={}&interval={}&since={}'.format(
    #    pair, interval, since)
    ex_df = pd.DataFrame(columns=[labels_dohlcv])
    url = 'https://api.kraken.com/0/public/OHLC?pair={}&interval={}&since={}'.format(
        pair, interval, since)
    page = requests.get(url)
    data = page.json()
    result = data["result"]
    if not result:
        return (ex_df, last)
    pair_name = [k for k in list(result.keys()) if k != 'last'][0]
    data = result[pair_name]
    last = result["last"]
    for i in range(len(data)):
        time, o, h, l, c, vwap, vol, count = data[i]
        date = nixtime_to_date(time)
        ex_df.loc[i] = [date, o, h, l, c, vol]
    return (ex_df, last)

def concat_kraken_data(pair, ccy, currency, since=0):
    labels = ['Date']
    labels.extend([ccy.upper()+i+currency.upper() 
        for i in ['_open_','_high_','_low_','_close_','_volume_']])
    ex_df = pd.DataFrame(columns=[labels])
    for i in range(10):
        print(since)
        this_df, since = download_kraken_pair(pair, labels, since)
        if this_df.empty:
            return ex_df
        ex_df = pd.concat([ex_df, this_df], ignore_index=True)
        time.sleep(1)
    return ex_df


def parse_bitcoincharts(fp, new_fp): 
    exchange = pd.read_csv(fp)
    columns = ['Date', 'BTC_avg_USD', 'BTC_volume_USD']
    exchange.columns = columns 
    exchange['Date'] = exchange['Date'].apply(nixtime_to_date)
    exchange['Date'] = pd.to_datetime(exchange.Date)
    
    ex_df = pd.DataFrame(columns=columns)
    dates = {k:[0, 0, 0] for k in exchange['Date'].dt.strftime('%Y-%m-%d').tolist()}
    for index, row in exchange.iterrows():
        date = row["Date"].strftime("%Y-%m-%d")
        dates[date][0] += 1
        dates[date][1] += row["BTC_avg_USD"]
        dates[date][2] += row["BTC_volume_USD"]
    print(dates)
    for k in dates:
        v = dates[k]
        avg = v[1] / v[0]
        vol = v[2]
        print(k, avg, vol)
        ex_df.append([k, avg, vol])
    
    for date in dates:
        day = exchange.loc[exchange['Date'].dt.strftime('%Y-%m-%d') == date]
        avg = day['BTC_avg_USD'].sum() / len(day)
        vol = day['BTC_volume_USD'].sum()
        ex_df.append([date, avg, vol])

    ex_df = exchange.sort_values(by='Date', ascending=0)
    ex_df.to_csv(new_fp, index=False)


if __name__ == "__main__":
    download_cryptocompare_csvs()
    #reorder('csvs', 'sorted')
    #print(concat_kraken_data('XBTUSD', 'BTC', 'USD'))
    #parse_bitcoincharts('other_csvs/krakenUSD.csv', 'other_csvs/krakenUSD.new')
