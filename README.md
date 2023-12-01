# ReadCSVFile

This Scala application reads a CSV file containing tweet data and performs several analyses and statistical calculations.

## Setup

The application uses Apache Spark for data processing. Ensure you have the required dependencies installed.

## Usage

1. The application reads a CSV file containing tweet data from `elonmusk_tweets.csv`.
2. Upon running the program, it will prompt the user to input a comma-separated list of keywords.
3. It then performs the following analyses:

### Analyses Performed:

#### 1. Distribution of Keywords Over Time
   - Shows the number of times each keyword is mentioned every day.
   - Displays output in the format: (keyword, date, count)

#### 2. Percentage of Tweets with at Least One Keyword
   - Calculates the percentage of tweets that contain at least one input keyword.

#### 3. Percentage of Tweets with Exactly Two Keywords
   - Determines the percentage of tweets containing exactly two input keywords.

#### 4. Tweet Length Analysis
   - Calculates the average length of tweets and their standard deviation.

### How to Run
   - Run the `ReadCSVFile` object's `main` method to execute the program.
   - Enter a comma-separated list of keywords when prompted.
   - The program will output the results of the analyses.

### Important Notes:
   - The program utilizes Apache Spark to process the CSV file.
   - Ensure the `elonmusk_tweets.csv` file is located at `src/main/resources/elonmusk_tweets.csv`.
   - The application calculates various statistics based on tweet data and displays them in the console.

## Libraries Used:
- Apache Spark
- Apache log4j

## Disclaimer:
This application assumes the presence of the specified CSV file and may require adjustments based on the input data format.

