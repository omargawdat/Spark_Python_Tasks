from base_class import DataProcessingTask


class Question4(DataProcessingTask):
    def process_data_with_loops(self, rdd):
        title_counts = {}

        # Processing each line to extract titles and count their occurrences
        for line in rdd.toLocalIterator():
            fields = line.split(" ")
            if len(fields) > 1:
                title = fields[1]  # The title is typically the second field
                title_counts[title] = title_counts.get(title, 0) + 1

        # Sorting the counts to find the most frequent titles
        sorted_counts = sorted(title_counts.items(), key=lambda item: item[1], reverse=True)

        # Formatting results to be saved
        results = "\n".join(f"{title}: {count}" for title, count in sorted_counts)
        return results

    def process_data_with_mapreduce(self, rdd):
        # MapReduce to count frequency of each title
        freq_title = rdd \
            .map(lambda line: (line.split(" ")[1], 1) if len(line.split(" ")) > 1 else (line, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda pair: pair[1], False)

        # Formatting results for output
        results = "\n".join(f"{title}: {count}" for title, count in freq_title.collect())
        return results

