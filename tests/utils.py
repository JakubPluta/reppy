import os.path
import random
import csv

root = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

def generate_sample_data(
    num_columns: int, num_rows: int, file_name: str
) -> None:
    """Generates sample data and stores it in a CSV file.

    Parameters
    ----------
    num_columns: int
        The number of columns in the sample data.
    num_rows: int
        The number of rows in the sample data.
    file_name: str
        The file name of the CSV file to store the sample data.

    Returns
    -------
    None
    """
    file_name = os.path.join(root, file_name)
    with open(file_name, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        header = [f"column_{x}" for x in range(num_columns)]
        writer.writerow(header)
        for row in range(num_rows):
            data = []
            for column in range(num_columns):
                data.append(str(random.randint(0, 100)))
            writer.writerow(data)


if __name__ == "__main__":
    num_columns = 15 # int(input("Number of columns: "))
    num_rows = 500000 #int(input("Number of rows: "))
    file_name = input("File name: ")
    generate_sample_data(num_columns, num_rows, file_name)
    print(f"Sample data saved to {file_name}")