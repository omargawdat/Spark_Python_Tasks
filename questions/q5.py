from base_class import DataProcessingTask


class Question5(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=5)

    def process_data_with_loops(self, rdd):
        title_data = rdd.map(lambda x: (x.split(" ")[1], x)).collect()
        combined_data = {}
        for title, data in title_data:
            if title not in combined_data:
                combined_data[title] = []
            combined_data[title].append(data)

        results = ""
        for title, datas in combined_data.items():
            formatted_datas = "\n".join([f"    {data}" for data in datas])
            results += f"{title}:\n{formatted_datas}\n"

        return results

    def process_data_with_mapreduce(self, rdd):
        combined_rdd = rdd \
            .map(lambda x: (x.split(" ")[1], x)) \
            .groupByKey() \
            .map(lambda x: (x[0], list(x[1])))
        return combined_rdd.map(lambda x: f"{x[0]}:\n" + "\n".join(x[1]))