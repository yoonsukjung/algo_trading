# Algorithmic Trading

This repository contains the implementation of an algorithmic trading system.

## Folder Structure

- `src/`: Contains the main source code for the trading system.
  - `__init__.py`: Initialize the package.
  - `backtester.py`: Implements the backtesting functionality.
  - `data_loader.py`: Handles data loading operations.
  - `main.py`: Main entry point for running the trading system.
  - `result_handler.py`: Manages the saving and handling of trading results.
  - `strategy.py`: Defines the trading strategy.
  - `utils.py`: Contains utility functions.
- `test/`: Contains the test cases for the trading system.

## Requirements

The required dependencies for running the project can be found in the `requirements.txt` file.

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yoonsukjung/algo_trading.git
    cd algo_trading
    ```

2. Install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

To run the backtesting, execute the following command:
```bash
python src/main.py
```

## Modules Description

### Backtester
- `backtester.py`: Provides backtesting functionality for the trading strategy.

### Data Loader
- `data_loader.py`: Loads the required data for the trading strategy.

### Main
- `main.py`: Main script to run the trading system.

### Result Handler
- `result_handler.py`: Saves and manages the trading results.

### Strategy
- `strategy.py`: Defines the core trading strategy.

### Utils
- `utils.py`: Contains utility functions such as setting up logging.

## Logging

Logging is set up in `utils.py` and used across different modules for tracking the execution flow and debugging.

## Contributing

Feel free to fork this repository, make updates and create pull requests. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License.
