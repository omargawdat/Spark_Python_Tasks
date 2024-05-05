from abc import ABC, abstractmethod
import os
import shutil
import time
from pyspark import SparkConf, SparkContext


class DataProcessingTask(ABC):
    def __init__(self, question_number):
        conf = SparkConf().setAppName(f"Question{question_number}Processing")
        self.sc = SparkContext.getOrCreate(conf=conf)
        self.question_number = question_number
        base_path = os.path.abspath("results")
        self.loop_result_path = os.path.join(base_path, f"q{question_number}/loop")
        self.mapreduce_result_path = os.path.join(base_path, f"q{question_number}/mapreduce")
        self.ensure_directory(self.loop_result_path)
        self.ensure_directory(self.mapreduce_result_path)

    @staticmethod
    def delete_existing_directory(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    @staticmethod
    def ensure_directory(path):
        os.makedirs(path, exist_ok=True)

    def read_data(self):
        input_file = os.path.join("data", "pagecounts-20160101-000000_parsed.out")
        return self.sc.textFile(input_file)

    @abstractmethod
    def process_data_with_loops(self, rdd):
        pass

    @abstractmethod
    def process_data_with_mapreduce(self, rdd):
        pass

    def save_results(self, results, method):
        result_path = self.loop_result_path if method == "Loop" else self.mapreduce_result_path
        self.delete_existing_directory(result_path)  # Ensure directory is removed if it exists

        if isinstance(results, str):
            os.makedirs(result_path, exist_ok=True)
            with open(os.path.join(result_path, "results.txt"), 'w') as file:
                file.write(results)
        else:
            results.saveAsTextFile(result_path)

    def execute_with_loops(self):
        print(f"{self.__class__.__name__}: Executing with loops")
        rdd = self.read_data()
        start_time = time.time()
        results = self.process_data_with_loops(rdd)
        end_time = time.time()
        print(f"Elapsed time with loops: {end_time - start_time} seconds")
        self.save_results(results, "Loop")

    def execute_with_mapreduce(self):
        print(f"{self.__class__.__name__}: Executing with MapReduce")
        rdd = self.read_data()
        start_time = time.time()
        results = self.process_data_with_mapreduce(rdd)
        end_time = time.time()
        print(f"Elapsed time with MapReduce: {end_time - start_time} seconds\n")
        self.save_results(results, "MapReduce")


