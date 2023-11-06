"""
Implement library functions
"""

from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    transform,
    start_spark,
    end_spark,
)


def main():
    # extract data
    extract()
    # start spark session
    spark = start_spark("NewVoters")
    # load data into dataframe
    df = load_data(spark)
    # example metrics
    describe(df)
    # query
    query(
        spark,
        df,
        "SELECT DISTINCT YEAR, SUM(NEW_VOTERS) \n\
        AS newvoter_jan_may FROM voters GROUP BY YEAR\n",
        "voters",
    )
    # example transform
    transform(df)
    # end spark session
    end_spark(spark)


if __name__ == "__main__":
    main()
