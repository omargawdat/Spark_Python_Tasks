
from base_class import DataProcessingTask


class Question1(DataProcessingTask):
    def process_data_with_loops(self, rdd):
        page_sizes = [int(line.split(" ")[-1]) if line.split(" ")[-1] != 'null' else 0
                      for line in rdd.toLocalIterator() if len(line.split(" ")) > 1]

        min_page_size = min(page_sizes) if page_sizes else 0
        max_page_size = max(page_sizes) if page_sizes else 0
        avg_page_size = sum(page_sizes) / len(page_sizes) if page_sizes else 0

        results = (f"Minimum page size: {min_page_size}, "
                   f"Maximum page size: {max_page_size}, "
                   f"Average page size: {avg_page_size}")
        return results

    def process_data_with_mapreduce(self, rdd):
        # Transform the incoming RDD by filtering and mapping to page sizes as integers
        page_size_rdd = rdd \
            .map(lambda line: line.split(" ")) \
            .filter(lambda parts: len(parts) > 1 and parts[-1] != 'null') \
            .map(lambda parts: int(parts[-1]))

        # Use Spark's built-in functions to calculate min, max, and mean
        min_page_size = page_size_rdd.min()
        max_page_size = page_size_rdd.max()
        avg_page_size = page_size_rdd.mean()

        # Create an RDD from the results to be compatible with saveAsTextFile
        results = self.sc.parallelize([
            f"Minimum page size (MR): {min_page_size}",
            f"Maximum page size (MR): {max_page_size}",
            f"Average page size (MR): {avg_page_size}"
        ])

        return results

