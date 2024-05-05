from base_class import DataProcessingTask


class Question1(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=1)

    def process_data_with_loops(self, rdd):
        page_size_pairs = rdd.flatMap(lambda line: [
            ('key', int(line.split(" ")[-1])) if line.split(" ")[-1] != 'null' and len(line.split(" ")) > 1 else (
                'key', 0)])
        grouped = page_size_pairs.groupByKey().mapValues(list)
        results = grouped.map(lambda x: (min(x[1]), max(x[1]), sum(x[1]) / len(x[1])) if x[1] else (0, 0, 0.0))
        min_size, max_size, avg_size = results.collect()[0] if results.collect() else (0, 0, 0.0)
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
