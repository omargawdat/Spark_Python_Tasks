
from base_class import DataProcessingTask


class Question2(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=2)

    def process_data_with_loops(self, rdd):
        data = rdd.map(lambda x: (x.split(" ")[0], x.split(" ")[1])).collect()
        count_the = sum(1 for code, title in data if title.startswith("The"))
        non_en_count = sum(1 for code, title in data if title.startswith("The") and not code.startswith("en"))
        return f"Titles starting with 'The': {count_the}\nTitles starting with 'The' and not in English project: {non_en_count}"

    def process_data_with_mapreduce(self, rdd):
        filtered_titles = rdd.map(lambda x: x.split(" ")) \
            .filter(lambda x: x[1].startswith("The"))
        count_the = filtered_titles.count()
        non_en_count = filtered_titles.filter(lambda x: not x[0].startswith("en")).count()
        return f"Titles starting with 'The': {count_the}\nTitles starting with 'The' and not in English project: {non_en_count}"
