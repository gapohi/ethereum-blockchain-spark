from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, DoubleType, LongType, StructField, StructType, StringType
from web3 import Web3

"""
gapohi 2025.

This is the main script of a project that fetches Ethereum blockchain transaction data and Tether ERC-20 transfer logs and stores
it as Spark DataFrames for exploratory data analysis. The research analyses various aspects such as the transaction volumes, top 
transactions by value, duplicated hashes, activity peaks, gas fees, mixers use, time between blocks, frequent addresses, and other...

!!!Disclaimer on Financial Decisions:!!!

The use of this software and the provided data should not be considered as financial 
advice. No responsibility is assumed for any financial decisions made based on the 
data or results obtained from the software. Users are responsible for conducting their 
own research and making informed decisions.
"""

def get_transactions(w3, from_block, latest_block, spark):
    """
    Fetches blockchain transactions and computes gas fees.
    """
    transactions = []
    wei_to_eth = 10**18 ## conversion factor from Wei to Ether
    ## loop through blocks in the specified range
    for block_number in range(from_block, latest_block + 1):
        try:
            block = w3.eth.get_block(block_number, full_transactions=True) ## fetch block with full transactions
        except Exception as e:
            raise Exception(f"Error fetching block {block_number}: {e}")
        ## loop through each transaction in the block
        for tx in block.transactions:
            try:
                tx_receipt = w3.eth.get_transaction_receipt(tx['hash'])
                gas_price = tx['gasPrice'] / wei_to_eth ## convert gas price to Ether
                gas_used = tx_receipt['gasUsed'] ## get the gas used in the transaction
                gas_fee = gas_price * gas_used ## calculate the gas fee
                ## append relevant transaction data to the transactions list
                transactions.append({
                    "block_number": block_number,
                    "timestamp": block.timestamp,
                    "from_address": tx['from'], 
                    "to_address": tx['to'],
                    "value": tx['value'] / wei_to_eth, ## convert value in wei to eth
                    "transaction_hash": tx['hash'].hex(), ## every transaction has a unique hash
                    "gas_price": gas_price,
                    "gas_used": gas_used,
                    "gas_fee": gas_fee
                })
            except Exception as e:
                raise Exception(f"Error fetching block transactions for block {block_number}: {e}")
    ## define Spark schema for the transaction data
    schema_block = StructType([
        StructField("block_number", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("transaction_hash", StringType(), True),
        StructField("gas_price", DoubleType(), True),
        StructField("gas_used", LongType(), True),
        StructField("gas_fee", DoubleType(), True)
    ])
    ## return the transaction data as a Spark DataFrame
    return spark.createDataFrame(transactions, schema=schema_block)

def get_logs_tether(w3, from_block, latest_block, spark):
    """
    Fetches Tether ERC-20 transfer logs.
    """
    ## erc-20 transfers have a specific address and event signature
    contract_address = w3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7")  ## ERC-20 (Tether)
    transfer_event_signature = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex() ## transfer event signature
    ## set filter parameters for logs
    filter_params = {
        'fromBlock': from_block,
        'toBlock': latest_block,
        'address': contract_address,
        'topics': [transfer_event_signature]
    }

    try:
        logs = w3.eth.get_logs(filter_params) ## fetch logs based on filter parameters
    except Exception as e:
        raise Exception(f"Error fetching logs: {e}")
    
    logs_tether = []
    ## loop through each log
    for log in logs:
        try:
            value_in_wei = int.from_bytes(log['data'], byteorder='big') ## convert data to value in Wei
            value_usdt = round(w3.from_wei(value_in_wei, 'mwei'), 2) ## convert Wei to USDT
            value_usdt_decimal = Decimal(str(value_usdt)) ## convert to Decimal
            block_info = w3.eth.get_block(log['blockNumber']) ## fetch block info
            block_timestamp = block_info.timestamp
            log = {
                "block_number": log['blockNumber'],
                "timestamp": block_timestamp,
                "from_address": "0x" + log['topics'][1].hex()[-40:], ## extracts the last 40 characters (20 bytes) of the topic's hex, representing an Ethereum address
                "to_address": "0x" + log['topics'][2].hex()[-40:],
                "value_usdt": value_usdt_decimal,
                "transaction_hash": log['transactionHash'].hex() ## every transaction has a unique hash
            }
            ## append log to list
            logs_tether.append(log)
        except Exception as e:
            raise Exception(f"Error processing log {log['transactionHash']}: {e}")
    ## define Spark schema for the log data
    schema_logs = StructType([
        StructField("block_number", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("value_usdt", DecimalType(38, 2), True),
        StructField("transaction_hash", StringType(), True)
    ])
    ## return the log data as a Spark DataFrame
    return spark.createDataFrame(logs_tether, schema=schema_logs)

def transactions_queries(spark):
    """
    Performs various SQL queries on transaction data to analyze transaction volumes, top transactions, duplicated hashes, activity peaks, gas fees, and more. 
    """
    ## count total number of transactions
    total_transactions = spark.sql("""
        SELECT COUNT(*) as n_total_transactions
        FROM df_transactions_complete;
    """)
    print('Number of total transactions:\n')
    total_transactions.show()

    ## get top 10 transactions by transaction value (ETH)
    tx_value_eth_top_10 = spark.sql("""
        SELECT 
            block_number, 
            timestamp, 
            transaction_hash, 
            tx_from_address, 
            tx_to_address, 
            tx_value_eth
        FROM df_transactions_complete
        ORDER BY tx_value_eth DESC
        LIMIT 10;                      
    """)
    print('Top 10 transactions by value:\n')
    tx_value_eth_top_10.show()

    ## get duplicated transaction hashes
    duplicated_transaction_hash = spark.sql("""
        SELECT transaction_hash, COUNT(*) AS n_transactions
        FROM df_transactions_complete
        GROUP BY transaction_hash
        HAVING n_transactions > 1
        ORDER BY n_transactions DESC
        LIMIT 10;
    """)
    print('Duplicated transaction hash:\n')
    duplicated_transaction_hash.show(truncate=False)

    ## get top 10 activity peaks by timestamp
    activity_peaks_top_10 = spark.sql("""
        SELECT timestamp, COUNT(*) AS n_transactions
        FROM df_transactions_complete
        GROUP BY timestamp
        ORDER BY n_transactions DESC
        LIMIT 10;
    """)
    print('Activity peaks: top 10 timestamps by number of transactions:\n')
    activity_peaks_top_10.show()

    ## get top 10 transactions by gas fee
    gas_fee_top_10 = spark.sql("""
        SELECT 
            transaction_hash,
            tx_value_eth,
            tx_gas_price_eth,
            tx_gas_used,
            tx_gas_fee_eth
        FROM df_transactions_complete
        ORDER BY tx_gas_fee_eth DESC
        LIMIT 10;
    """)
    print('Top 10 transactions by gas fee:\n')
    gas_fee_top_10.show(truncate=False)

    ## check if the sender's address (tx_from_address) is associated with known mixers  
    ## mixers are services that obscure the origin of cryptocurrency transactions by pooling and redistributing funds
    ## these are some examples of mixers, may be obsolete
    mixers_use_from = spark.sql("""
        SELECT 
            tx_from_address,
            COUNT(*) AS n_transactions
        FROM df_transactions_complete
        WHERE 
            tx_from_address in (
            '0x5f4b5e8e6894e7b94b9b0d7b28b8011c18c04e1d',
            '0x8fa7a7e8f79d3de88f8f3cfe69325425f94c12cf',
            '0x0e83c9f1ec5f9d7a028b963907f490b30709d12c',
            '0x9d1f522d5869c9b7ad864a29eb6e9f57a170d049',
            '0x39ccecc9ebcc4ad3132a99893b28d7c890810149'
            )
        GROUP BY tx_from_address
        ORDER BY n_transactions DESC;
    """)
    print('Mixers use by from_address:\n')
    mixers_use_from.show(truncate=False)

    ## check if the receiver's address (tx_to_address) is associated with know mixers
    mixers_use_to = spark.sql("""
        SELECT
            tx_to_address,
            COUNT(*) AS n_transactions
        FROM df_transactions_complete
        WHERE 
            tx_to_address in (
            '0x5f4b5e8e6894e7b94b9b0d7b28b8011c18c04e1d',
            '0x8fa7a7e8f79d3de88f8f3cfe69325425f94c12cf',
            '0x0e83c9f1ec5f9d7a028b963907f490b30709d12c',
            '0x9d1f522d5869c9b7ad864a29eb6e9f57a170d049',
            '0x39ccecc9ebcc4ad3132a99893b28d7c890810149'
            )
        GROUP BY tx_to_address
        ORDER BY n_transactions DESC;
    """)
    print('Mixers use by to_address:\n')
    mixers_use_to.show(truncate=False)

    ## calculate time between blocks
    time_between_blocks = spark.sql("""
    WITH grouped_data AS (
        SELECT 
            block_number,
            timestamp,
            MAX(unix_timestamp) AS unix_timestamp
        FROM df_transactions_complete
        GROUP BY block_number, timestamp
        ORDER BY block_number, timestamp
    ),
    timediff_data AS (
        SELECT
            block_number,
            timestamp,
            unix_timestamp,
            LAG(unix_timestamp) OVER (ORDER BY block_number) as unix_prev_timestamp
        FROM grouped_data
    )
    SELECT 
        block_number,
        timestamp,
        unix_timestamp,
        unix_prev_timestamp,
        COALESCE((unix_timestamp-unix_prev_timestamp), 0) AS time_diff_in_seconds
    FROM timediff_data;
    """)
    time_between_blocks.createOrReplaceTempView("block_times")
    print('Time between blocks:\n')
    time_between_blocks.show(10)

    ## monitor blocks with more than 12 seconds of difference (possible network errors)
    block_times_monitoring = spark.sql("""
    SELECT 
        block_number,
        timestamp,
        unix_timestamp,
        unix_prev_timestamp
    FROM block_times
    WHERE time_diff_in_seconds > 12;
    """)
    print('Blocks with more than 12 seconds of difference from the previous block (possible network errors):\n')
    block_times_monitoring.show()

    ## identify sender addresses with high transaction frequency
    from_address_freq = spark.sql("""
    SELECT 
        tx_from_address, COUNT(*) AS n_transactions
    FROM df_transactions_complete
    GROUP BY tx_from_address
    HAVING n_transactions > 1
    ORDER BY n_transactions DESC
    LIMIT 10;
    """)
    print('Highly repeated directions (from):\n')
    from_address_freq.show(truncate=False)

    ## identify receiver addresses with high transaction frequency
    to_address_freq = spark.sql("""
    SELECT 
        tx_to_address, COUNT(*) AS n_transactions
    FROM df_transactions_complete
    GROUP BY tx_to_address
    HAVING n_transactions > 1
    ORDER BY n_transactions DESC
    LIMIT 10;
    """)
    print('Highly repeated directions (to):\n')
    to_address_freq.show(truncate=False)

    return

def tether_queries(spark):
    """
    Performs various SQL queries on tether transactions data to analyze transaction volumes, top transactions, activity peaks, gas fees, and more.
    """
    ## count total number of Tether - ERC20 transactions
    erc_20_total_transactions = spark.sql("""
        SELECT COUNT(*) as n_total_transactions
        FROM df_logs_tether_clean;
    """)
    print('Number of Tether - ERC20 total transactions:\n')
    erc_20_total_transactions.show()

    ## get top 10 transactions by value (USDT)
    log_value_top_10 = spark.sql("""
        SELECT 
            block_number, 
            timestamp, 
            transaction_hash, 
            log_from_address, 
            log_to_address, 
            log_value_usdt
        FROM df_logs_tether_clean
        ORDER BY log_value_usdt DESC
        LIMIT 10;                      
    """)
    print('Tether Top 10 transactions by value (USDT):\n')
    log_value_top_10.show()

    ## get top 10 activity peaks by number of transactions
    log_activity_peaks_top_10 = spark.sql("""
        SELECT timestamp, COUNT(*) AS n_transactions
        FROM df_logs_tether_clean
        GROUP BY timestamp
        ORDER BY n_transactions DESC
        LIMIT 10;
    """)
    print('Activity peaks in Tether: Top 10 timestamps by number of transactions:\n')
    log_activity_peaks_top_10.show()

    ## find top 10 most frequent 'from' addresses
    log_from_address_freq = spark.sql("""
    SELECT 
        log_from_address, COUNT(*) AS n_transactions
    FROM df_logs_tether_clean
    GROUP BY log_from_address
    HAVING n_transactions > 1
    ORDER BY n_transactions DESC
    LIMIT 10;
    """)
    print('Tether Highly repeated directions (from):\n')
    log_from_address_freq.show(truncate=False)

    ## find top 10 most frequent 'to' addresses
    log_to_address_freq = spark.sql("""
    SELECT 
        log_to_address, COUNT(*) AS n_transactions
    FROM df_logs_tether_clean
    GROUP BY log_to_address
    HAVING n_transactions > 1
    ORDER BY n_transactions DESC
    LIMIT 10;
    """)
    print('Tether Highly repeated directions (to):\n')
    log_to_address_freq.show(truncate=False)

    ## gas fee comparison between general and tether transactions
    gas_comparison = spark.sql("""
    SELECT 
        'general_transactions' AS table,
        MIN(tx_gas_fee_eth) AS min_gas_fee,
        MAX(tx_gas_fee_eth)AS max_gas_fee,
        AVG(tx_gas_fee_eth)AS avg_gas_fee,
        STDDEV(tx_gas_fee_eth) AS stddev_gas_fee
    FROM df_transactions_complete
    WHERE log_value_usdt IS NULL 
    UNION ALL
    SELECT 
        'tether_transactions' AS table,
        MIN(tx_gas_fee_eth) AS min_gas_fee,
        MAX(tx_gas_fee_eth)AS max_gas_fee,
        AVG(tx_gas_fee_eth)AS avg_gas_fee,
        STDDEV(tx_gas_fee_eth) AS stddev_gas_fee
    FROM df_logs_tether_clean;
    """)
    print('General transactions gas fee vs Tether gas fee:\n')
    gas_comparison.show(truncate=False)

    ## count number of burning Tether logs
    ## "burning" refers to sending tokens to a null address, effectively removing them from circulation to reduce supply
    burning = spark.sql("""
    SELECT COUNT(*) as n_burning_logs
    FROM df_logs_tether_clean
    WHERE log_to_address = '0x0000000000000000000000000000000000000000'
        AND log_from_address != '0x0000000000000000000000000000000000000000';
    """)
    print('Number of burning Tether logs:\n')
    burning.show()

    return

def main():
    ## create Spark session and set log level to ERROR
    spark = SparkSession.builder.appName("Ethereum Transactions").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    ## connect to Ethereum network using Alchemy provider
    w3 = Web3(Web3.HTTPProvider("https://eth-mainnet.g.alchemy.com/v2/PERSONAL_KEY_TO_ALCHEMY"))
    ## check if the connection to Ethereum is successful
    if w3.is_connected():
        print("✅ Connected to Ethereum\n")
    else:
        print("❌ Connection failed\n")

    ## get the latest block number and define range of blocks to analyze
    latest_block = w3.eth.block_number
    from_block = latest_block - 9 ## select number of blocks

    ## get Ethereum transactions and create a Spark temporary view
    df_transactions = get_transactions(w3, from_block, latest_block, spark)
    df_transactions.createOrReplaceTempView("transactions")

    ## get tether logs and create a Spark temporary view
    df_logs_tether = get_logs_tether(w3, from_block, latest_block, spark)
    df_logs_tether.createOrReplaceTempView("logs_tether")
    
    ## join transactions with tether logs and create a new view for complete transaction data
    df_transactions_complete = spark.sql("""
        SELECT 
            t.block_number AS block_number, 
            t.timestamp AS unix_timestamp,
            from_utc_timestamp(FROM_UNIXTIME(t.timestamp), 'Europe/Madrid') AS timestamp, 
            t.transaction_hash AS transaction_hash,
            t.from_address AS tx_from_address, 
            t.to_address AS tx_to_address,
            ROUND(t.value, 2) AS tx_value_eth, 
            ROUND(t.gas_price, 12) AS tx_gas_price_eth,
            t.gas_used AS tx_gas_used,
            ROUND(t.gas_fee, 12) AS tx_gas_fee_eth,                                                                  
            l.from_address AS log_from_address, 
            l.to_address AS log_to_address, 
            l.value_usdt AS log_value_usdt
        FROM transactions AS t
        LEFT JOIN logs_tether AS l
            ON t.transaction_hash = l.transaction_hash 
            AND t.block_number = l.block_number 
            AND t.timestamp = l.timestamp;
    """)
    df_transactions_complete.createOrReplaceTempView("df_transactions_complete")

    ## filter the tether transactions to only include tether logs
    df_logs_tether_clean = spark.sql("""
        SELECT *
        FROM df_transactions_complete
        WHERE log_value_usdt IS NOT NULL;                      
    """)
    df_logs_tether_clean.createOrReplaceTempView("df_logs_tether_clean")

    print('\n')
    print('------------------------')
    print('GENERAL ETH TRANSACTIONS')
    print('------------------------')
    print('\n')
    transactions_queries(spark) ## execute general Ethereum transactions queries

    print('\n')
    print('-------------')
    print('ERC-20 TETHER')
    print('-------------')
    print('\n')
    tether_queries(spark) ## execute Tether-related queries

    return

if __name__ == "__main__":
    main()