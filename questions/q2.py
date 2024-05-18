from base_class import DataProcessingTask


class Question2(DataProcessingTask):
    def __init__(self):
        super().__init__(question_number=2)

    def process_data_with_loops(self, rdd):
        # Collect the RDD to the driver node
        data = rdd.collect()

        # Initialize counters
        count_the = 0
        non_en_count = 0

        # Iterate through the collected data
        for line in data:
            parts = line.split(" ")
            if len(parts) > 1:
                project_code = parts[0]
                page_title = parts[1]
                if page_title.startswith("The"):
                    count_the += 1
                    if not project_code.startswith("en"):
                        non_en_count += 1

        output = f"Titles starting with 'The': {count_the}\nTitles starting with 'The' and not in English project: {non_en_count}"
        return output
    def process_data_with_mapreduce(self, rdd):
        filtered_titles = rdd.map(lambda x: x.split(" ")) \
            .filter(lambda x: x[1].startswith("The"))
        count_the = filtered_titles.count()
        non_en_count = filtered_titles.filter(lambda x: not x[0].startswith("en")).count()
        return f"Titles starting with 'The': {count_the}\nTitles starting with 'The' and not in English project: {non_en_count}"
