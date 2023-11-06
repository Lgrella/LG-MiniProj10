# Data Engineering Systems: Mini Project 10
# Pyspark and SparkSQL

## Getting Started
* Open codespaces or clone repository, run make install (if needed) to install dependencies.
* Run main.py to implement data processing and analysis on a sample dataset using Spark (New Voters 2016/2020)

## Checking the Code
* Make lint, make format, and make test are available to test and format code, and confirm it is working as expected

## Implementation Process: ETL and Simple Querying
1. Extract dataset from online source (In this case: 538 Github data repository)
2. Start spark session named `NewVoters`
3. Load the extracted dataset into this spark session
4. Get summary statistics of dataset
5. Perform a simple Query of the dataset
6. Perform a transformation of the dataset, in this case I add a new column identifying each record as Winter or Spring
7. Lastly, I end the `NewVoters` spark session

## Evidence of Successful Performance
[Here](minilab10_output.md) you will find the log of each of the above steps running. The corresponding output is printed, with the step clearly labeled.
