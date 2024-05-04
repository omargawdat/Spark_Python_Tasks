def execute_question(question_number, execution_type):
    # Map numbers to methods
    methods = {
        '1': 'execute_with_loops',
        '2': 'execute_with_mapreduce'
    }

    # Dynamically create question instance based on input
    question_class = globals()[f"Question{question_number}"]
    question_instance = question_class(question_number)

    # Execute the selected method
    try:
        method_to_call = getattr(question_instance, methods[execution_type])
        method_to_call()
    except KeyError:
        print("Invalid execution type! Choose '1' for loop or '2' for mapreduce.")
    except AttributeError:
        print("Execution method not found!")


if __name__ == "__main__":
    while True:
        # Prompt user for the question number
        question_number = input("Enter the question number (1-5) or '0' to exit: ")
        if question_number == '0':
            print("Exiting program.")
            break

        if question_number.isdigit() and int(question_number) in range(1, 6):
            execution_type = input("Choose the execution type (1 for loop, 2 for mapreduce): ")
            if execution_type in ['1', '2']:
                execute_question(question_number, execution_type)
            else:
                print("Invalid execution type! Please enter '1' for loop or '2' for mapreduce.")
        else:
            print("Invalid question number! Please enter a number between 1 and 5 or '0' to exit.")
