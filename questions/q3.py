from base_class import DataProcessingTask


class Question3(DataProcessingTask):
    @staticmethod
    def normalize_and_split_title(title):
        # Normalize by converting to lowercase and removing non-alphanumeric characters
        normalized = ''.join(char.lower() for char in title if char.isalnum() or char == '_')
        return normalized.split('_')

    def process_data_with_loops(self, rdd):
        term_count = {}

        for line in rdd.toLocalIterator():
            parts = line.split(" ")
            if len(parts) >= 2:
                title = parts[1]
                terms = Question3.normalize_and_split_title(title)
                for term in terms:
                    if term:
                        term_count[term] = term_count.get(term, 0) + 1

        unique_terms_count = sum(1 for count in term_count.values() if count == 1)
        results = f"Number of unique terms: {unique_terms_count}"
        return results

    def process_data_with_mapreduce(self, rdd):
        # Normalize and split titles into terms
        terms = rdd.flatMap(
            lambda line: Question3.normalize_and_split_title(line.split(" ")[1] if len(line.split(" ")) >= 2 else ""))

        # Map terms to (term, 1), reduce by key to count occurrences, and filter for unique terms
        term_counts = terms.map(lambda term: (term, 1)).reduceByKey(lambda a, b: a + b)
        unique_terms_count = term_counts.filter(lambda term_count: term_count[1] == 1).count()

        results = self.sc.parallelize([(f"Number of unique terms (MR): {unique_terms_count}",)])
        return results

