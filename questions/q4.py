from base_class import DataProcessingTask


class Question4(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=4)

    def process_data_with_loops(self, rdd):
        titles = rdd.map(lambda x: x.split(" ")[1]).collect()
        title_count = {}
        for title in titles:
            if title in title_count:
                title_count[title] += 1
            else:
                title_count[title] = 1
        results = "\n".join([f"{title}: {count}" for title, count in title_count.items()])
        return results

    def process_data_with_mapreduce(self, rdd):
        title_count = rdd \
            .map(lambda x: x.split(" ")[1]) \
            .map(lambda title: (title, 1)) \
            .reduceByKey(lambda a, b: a + b)
        results = "\n".join(title_count.map(lambda pair: f"{pair[0]}: {pair[1]}").collect())
        return results
