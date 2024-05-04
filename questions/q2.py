from base_class import DataProcessingTask


# todo refactor cods to separate between loop and mapreduce
class Question2(DataProcessingTask):
    def process_data_with_loops(self, rdd):
        count_all_the = 0
        count_non_en_the = 0

        for line in rdd.toLocalIterator():
            parts = line.split(" ")
            if len(parts) >= 2 and parts[1].startswith('The_'):
                count_all_the += 1
                if parts[0] != 'en':
                    count_non_en_the += 1

        results = (f"Total 'The' page titles: {count_all_the}, "
                   f"Non-English 'The' page titles: {count_non_en_the}")
        return results

    def process_data_with_mapreduce(self, rdd):
        titles_with_the = rdd.map(lambda line: line.split(" ", 2)) \
            .filter(lambda parts: len(parts) >= 2 and parts[1].startswith('The_'))

        count_all_the = titles_with_the.count()
        non_en_titles_with_the = titles_with_the.filter(lambda parts: parts[0] != 'en').count()

        results = self.sc.parallelize([(f"Total 'The' page titles (MR): {count_all_the}",),
                                       (f"Non-English 'The' page titles (MR): {non_en_titles_with_the}",)])
        return results
