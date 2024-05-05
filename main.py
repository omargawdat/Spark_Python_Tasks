from questions.q1 import Question1
from questions.q2 import Question2
from questions.q3 import Question3
from questions.q4 import Question4
from questions.q5 import Question5

questions = {
    '1': Question1,
    '2': Question2,
    '3': Question3,
    '4': Question4,
    '5': Question5,
}


def execute_question(question_number):
    try:
        question_class = questions[question_number]
        question_instance = question_class()

        # Execute both methods
        print(f"Executing with loops for Question {question_number}")
        question_instance.execute_with_loops()

        print(f"Executing with MapReduce for Question {question_number}")
        question_instance.execute_with_mapreduce()
    except KeyError as e:
        print(f"Error: {str(e)} - Please check your input.")
    except AttributeError as e:
        print(f"Execution method not found! {str(e)}")


if __name__ == "__main__":
    while True:
        question_number = input("Enter the question number (1-5) or '0' to exit: ")
        if question_number == '0':
            print("Exiting program.")
            break

        if question_number.isdigit() and int(question_number) in range(1, 6):
            execute_question(question_number)
        else:
            print("Invalid question number! Please enter a number between 1 and 5 or '0' to exit.")
