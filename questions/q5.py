from base_class import DataProcessingTask


class Question5(DataProcessingTask):
    def process_data_with_loops(self, rdd):
        parsed_data = []

        # Extract relevant parts of each line for combining
        for line in rdd.toLocalIterator():
            fields = line.split(" ")
            if len(fields) > 2:
                title = fields[1]
                hinst = fields[2:-1]  # Extract hinst values
                size = fields[-1]  # Extract size value
                parsed_data.append((title, (hinst, [size])))

        # Combine data for the same title
        combined_data = {}
        for title, data in parsed_data:
            hinst, size = data
            if title not in combined_data:
                combined_data[title] = ([], [])
            combined_data[title][0].extend(hinst)
            combined_data[title][1].extend(size)

        # Format the results for saving
        results = "\n".join(f"Title: {title}, Hinst: {', '.join(data[0])}, Size: {', '.join(data[1])}"
                            for title, data in combined_data.items())
        return results

    def process_data_with_mapreduce(self, rdd):
        # Transform the data to key-value pairs and reduce by key
        combined_data = rdd.map(lambda line: (line.split(" ")[1], (line.split(" ")[2:-1], [line.split(" ")[-1]]))) \
                           .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                           .mapValues(lambda x: {"hinst": ", ".join(x[0]), "size": ", ".join(x[1])})

        # Format the results for output and saving
        results = "\n".join(f"Title: {x[0]}, Page data: Hinst: {x[1]['hinst']}, Size: {x[1]['size']}"
                            for x in combined_data.collect())
        return results


