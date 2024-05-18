from base_class import DataProcessingTask


class Question1(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=1)

    def process_data_with_loops(self, rdd):
        # Collect the RDD to the driver node
        data = rdd.collect()

        # Initialize variables
        min_size = float('inf')
        max_size = float('-inf')
        total_size = 0
        count = 0

        # Iterate through the collected data
        for line in data:
            parts = line.split(" ")
            if len(parts) > 1 and parts[-1] != 'null':
                page_size = int(parts[-1])
                min_size = min(min_size, page_size)
                max_size = max(max_size, page_size)
                total_size += page_size
                count += 1

        # Compute the average size
        if count > 0:
            avg_size = total_size / count
        else:
            min_size = max_size = avg_size = 0

        output = f"Minimum page size: {min_size}\nMaximum page size: {max_size}\nAverage page size: {avg_size:.2f}"
        return output

    def process_data_with_mapreduce(self, rdd):
        page_size_rdd = rdd \
            .map(lambda line: line.split(" ")) \
            .filter(lambda parts: len(parts) > 1 and parts[-1] != 'null') \
            .map(lambda parts: int(parts[-1]))

        min_page_size = page_size_rdd.min()
        max_page_size = page_size_rdd.max()
        avg_page_size = page_size_rdd.mean()

        output = f"Minimum page size: {min_page_size}\nMaximum page size: {max_page_size}\nAverage page size: {avg_page_size:.2f}"
        return output
