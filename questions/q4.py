from base_class import DataProcessingTask


class Question4(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=4)

    def process_data_with_loops(self, rdd):
        # Collect the RDD to the driver node
        data = rdd.collect()

        # Initialize a dictionary to store the title counts
        title_count = {}

        # Iterate through the collected data
        for line in data:
            parts = line.split(" ")
            if len(parts) > 1:
                title = parts[1]
                if title in title_count:
                    title_count[title] += 1
                else:
                    title_count[title] = 1

        # Format the results as a string
        results = "\n".join([f"{title}: {count}" for title, count in title_count.items()])
        return results


    def process_data_with_mapreduce(self, rdd):
        title_count = rdd \
            .map(lambda x: x.split(" ")[1]) \
            .map(lambda title: (title, 1)) \
            .reduceByKey(lambda a, b: a + b)
        results = "\n".join(title_count.map(lambda pair: f"{pair[0]}: {pair[1]}").collect())
        return results
