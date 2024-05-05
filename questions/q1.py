from base_class import DataProcessingTask


class Question1(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=1)

    def process_data_with_loops(self, rdd):
        # Collecting and processing data on the driver to simulate a loop
        page_size_pairs = rdd.flatMap(lambda line: [
            ('key', int(line.split(" ")[-1])) if line.split(" ")[-1] != 'null' and len(line.split(" ")) > 1 else (
                'key', 0)]).collect()

        # Creating a dictionary to simulate grouping by key
        grouped_sizes = {}
        for key, size in page_size_pairs:
            if key not in grouped_sizes:
                grouped_sizes[key] = []
            grouped_sizes[key].append(size)

        # Calculating min, max, and average sizes from the collected sizes
        if grouped_sizes['key']:
            sizes = grouped_sizes['key']
            min_size = min(sizes)
            max_size = max(sizes)
            avg_size = sum(sizes) / len(sizes)
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
