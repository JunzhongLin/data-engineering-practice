import logging
import findspark
findspark.init()
import os
import re
from argparse import ArgumentParser
from pathlib import Path
from zipfile import ZipFile
from typing import List

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, SparkSession, Window

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

parser = ArgumentParser()
parser.add_argument("--in_memory", action="store_true")

def create_directory(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)
    
def create_spark_dataframe_from_memory(
    list_str: list, sc: SparkSession, **kwargs
) -> DataFrame:
    p = sc.sparkContext.parallelize(list_str, numSlices=1000)
    return sc.read.csv(p, **kwargs)


def write_results(file_path: Path, df: DataFrame) -> None:
    if file_path.is_dir():
        df.repartition(1).write.mode("overwrite").csv(
                os.fspath(file_path), header=True
            )
    else:
        df.repartition(1).write.mode("append").csv(
                os.fspath(file_path), header=True
            )


def read_zipfile_content_to_memory(zipfile_path: Path):
    with ZipFile(zipfile_path, "r") as zip:
        return [zip.read(x) for x in zip.namelist() if re.fullmatch(r"[^/]*\.csv", x)]


def read_data_into_spark(zip_file_paths: List[Path], sc: SparkSession):
    csv_frames = {}

    for zip_file_path in zip_file_paths:

        zip_contents = [e for e in read_zipfile_content_to_memory(zip_file_path)]
        for csv_file in zip_contents:
            logger.info(f"Reading files in {zip_file_path} from memory")

            temp_sdf = create_spark_dataframe_from_memory(
                csv_file.decode("utf-8").splitlines(), sc, header=True
            )
            csv_frames[str(zip_file_path)] = temp_sdf.withColumn(
                "date",
                sf.to_date(*({"started_at", "start_time"} & set(temp_sdf.columns))),
            ).cache()
    return csv_frames


class QuestionAnsweringService:
    def __init__(self, csv_frames: dict, sc: SparkSession):
        self.csv_frames = csv_frames
        self.sc = sc

    def answer_question1(self, file_path: Path):
        """
        1. What is the `average` trip duration per day?
        """
        logging.info("Answering question 1")
        sdf_lists= []
        for sdf in self.csv_frames.values():
            start_time_col_name = (
                {"started_at", "start_time"} & set(sdf.columns)
            )
            end_time_col_name = (
                {"ended_at", "end_time"} & set(sdf.columns)
            )
            sdf = sdf.select(
                "date",
                (
                    sf.unix_timestamp(sf.to_timestamp(*start_time_col_name))
                    - sf.unix_timestamp(sf.to_timestamp(*end_time_col_name)) 
                ).alias("tripduration")
            ).groupby("date").agg(
                sf.mean("tripduration").alias("avg_trip_duration")
            )
            sdf_lists.append(sdf)
        res_df = sdf_lists[0]
        for i in range(len(sdf_lists)-1):
            res_df = res_df.union(sdf_lists[i+1])
        
        write_results(file_path, res_df)

    def answer_question2(self, file_path: Path):
        """
        2. How many trips were taken each day?
        """
        logging.info("Answering question 2")
        
        sdf_lists = []
        
        for sdf in self.csv_frames.values():
            sdf = sdf.groupBy("date").count()
            sdf_lists.append(sdf)
        
        res_df = sdf_lists[0]
        for i in range(len(sdf_lists)-1):
            res_df.union(sdf_lists[i+1])
        
        write_results(file_path, res_df)


    def answer_question3(self, file_path: Path):
        """
        3. What was the most popular starting trip station for each month?
        """
        logging.info("Answering question 3")

        sdf_list= []
        
        for sdf in self.csv_frames.values():
            sdf = (
                sdf.select(
                "*",
                *[
                    func(sf.col(x)).alias(y)
                    for x, y, func in zip(
                        ["date", "date"], ["year", "month"], [sf.year, sf.month]
                    )
                ],
                sf.col(
                    *{"start_station_id", "from_station_id"} & set(sdf.columns)
                ).alias("start_location"),
            )
            .groupby("year", "month", "start_location")
            .count()
            .orderBy(["year", "month", "count"], ascending=False)
            .groupby("year", "month")
            .agg(sf.first("start_location").alias("most_popular_start_location"))
            )
            sdf_list.append(sdf)
            
        res_df = sdf_list[0]
        for i in range(len(sdf_list)-1):
            res_df.union(sdf_list[i+1])
        
        write_results(file_path, res_df)

    def answer_question4(self, file_path: Path):
        """
        4. What were the top 3 trip stations each day for the last two weeks?
        """
        logging.info("Answering question 4")

        sdf = self.csv_frames["data/Divvy_Trips_2020_Q1.zip"]
        window = Window.partitionBy("date").orderBy(sf.col("count").desc())
        
        date_cutoff = sdf.select(
            sf.date_add(sf.max("date"), -14).alias("max_date")
        ).collect()[0]["max_date"]
        res_df = sdf.where(sf.col("date") >= date_cutoff).groupby(
            "date", "start_station_name"
        ).count().withColumn("rank", sf.row_number().over(window)).where(
            sf.col("rank") <= 3
        )
        write_results(file_path, res_df)
        
    def answer_question5(self, file_path: Path):
        """
        5. Do `Male`s or `Female`s take longer trips on average?
        """
        logging.info("Answering question 5")

        sdf = self.csv_frames["data/Divvy_Trips_2019_Q4.zip"]
        
        start_time_col_name = (
            {"started_at", "start_time"} & set(sdf.columns)
        )
        end_time_col_name = (
            {"ended_at", "end_time"} & set(sdf.columns)
        )
        
        sdf = sdf.withColumn("tripduration", 
                                (
                                sf.unix_timestamp(sf.to_timestamp(*start_time_col_name))
                                - sf.unix_timestamp(sf.to_timestamp(*end_time_col_name)) 
                                )
                            )
        
        res_df = sdf.dropna(subset=["tripduration", "gender"]).groupby("gender").agg(
            sf.mean("tripduration").alias("avg_tripduration")
        ).selectExpr(
            "max_by(gender, avg_tripduration) as longest_trip_takers"
        )
        
        write_results(file_path, res_df)

    def answer_question6(self, file_path: Path):
        """
        6. What is the top 10 ages of those that take the longest trips, and shortest
        """
        logging.info("Answering question 6")
        
        sdf = self.csv_frames["data/Divvy_Trips_2019_Q4.zip"]
        
        start_time_col_name = (
            {"started_at", "start_time"} & set(sdf.columns)
        )
        end_time_col_name = (
            {"ended_at", "end_time"} & set(sdf.columns)
        )
        
        sdf = sdf.withColumn("tripduration", 
                                (
                                sf.unix_timestamp(sf.to_timestamp(*start_time_col_name))
                                - sf.unix_timestamp(sf.to_timestamp(*end_time_col_name)) 
                                )
                            )
        
        res_df = sdf.orderBy(sf.desc("tripduration")).dropna(
            subset=["birthyear"]
        ).limit(10).select((2022 - sf.col("birthyear")).alias("age"))
        
        write_results(file_path, res_df)

def main():
    sc = (SparkSession.builder
          .appName("Exercise6")
          .config("spark.driver.memory", "3g")
          .getOrCreate())

    directory = "reports"
    create_directory(directory)

    zip_file_paths = list(Path().rglob("*.zip"))

    csv_frames = read_data_into_spark(zip_file_paths, sc)

    qas = QuestionAnsweringService(csv_frames, sc)
    qas.answer_question1(Path(f"{directory}/question1.csv"))
    qas.answer_question2(Path(f"{directory}/question2.csv"))
    qas.answer_question3(Path(f"{directory}/question3.csv"))
    qas.answer_question4(Path(f"{directory}/question4.csv"))
    qas.answer_question5(Path(f"{directory}/question5.csv"))
    qas.answer_question6(Path(f"{directory}/question6.csv"))

if __name__ == "__main__":
    main()
