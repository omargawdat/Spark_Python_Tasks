from base_class import DataProcessingTask
import re


class Question3(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=3)

    def process_data_with_loops(self, rdd):
        data = rdd.collect()

        unique_terms = set()

        for line in data:
            parts = line.split(" ")
            if len(parts) > 1:
                title = parts[1]
                normalized_terms = re.sub(r'[^a-zA-Z0-9_]', '', title.lower()).split("_")
                unique_terms.update(normalized_terms)

        return f"Number of unique terms in page titles: {len(unique_terms)}"

    def process_data_with_mapreduce(self, rdd):
        unique_terms_count = rdd \
            .map(lambda x: x.split(" ")[1]) \
            .flatMap(lambda title: re.sub(r'[^a-zA-Z0-9_]', '', title.lower()).split("_")) \
            .distinct() \
            .count()
        return f"Number of unique terms in page titles: {unique_terms_count}"
