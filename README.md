## Project Overview

This project fetches Ethereum blockchain transaction data and Tether ERC-20 transfer logs and stores
it as Spark DataFrames for exploratory data analysis. The research analyses various aspects such as 
the transaction volumes, top transactions by value, duplicated hashes, activity peaks, gas fees, mixers 
use, time between blocks, frequent addresses, and other...

Tools:
- Ubuntu	(Linux-based operating system for the project environment) -- 24.04.1
- Docker	(Container to set up Apache Spark and install Python dependencies) -- 27.2.0
- Python	(Main programming language for the application) -- 3.12.3
- PySpark	(Python library for efficient processing and exploration of large datasets) -- 3.5.4
- Web3		(Python library for Ethereum API blockchain interactions) -- 7.8.0

## Disclaimer on Financial Decisions

The use of this software and the provided data should not be considered as financial advice. No responsibility 
is assumed for any financial decisions made based on the data or results obtained from the software. Users are 
responsible for conducting their own research and making informed decisions.

## Directory Structure

Here’s an overview of the project directory structure:

```plaintext
ethereum-blockchain-spark/
├── src/
│   └── main.py          # Main entry point for running the project
├── .gitignore           # For ignoring unnecessary files in the project
├── Dockerfile           # Defines the environment for the project by setting up a Docker container
├── LICENSE              # MIT License
├── README.md            # Project overview and setup instructions
├── requirements         # List of python dependencies required for the project
```

## Installation

1. Clone the repository to your local Linux machine:
```bash
git clone https://github.com/gapohi/ethereum-blockchain-spark.git
```

2. Start Ubuntu.

3. Navigate to the repository directory:
```bash
cd path/to/repository/ethereum-blockchain-spark
```

4. Start Docker Desktop.

5. Build the Docker container with its dependencies:
```bash
docker build -t ethereum-blockchain-spark .
```

6. Run the main.py file:
```bash
docker run --rm -it -v ~/path/to/repository/ethereum-blockchain-spark:/app ethereum-blockchain-spark python3 /app/src/main.py
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would 
like to change. Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
