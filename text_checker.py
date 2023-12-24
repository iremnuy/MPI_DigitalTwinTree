import sys

#Basak Tepe 2020400117
#Irem Nur Yildirim 2020401042
#Group Number 38

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: python text_checker.py file_name1 file_name2")
        sys.exit(1)

    # Get file paths from command line arguments
    file_path1 = sys.argv[1]
    file_path2 = sys.argv[2]

    # Read the contents of the files
    with open(file_path1, 'r') as file1:
        text1 = file1.read()

    with open(file_path2, 'r') as file2:
        text2 = file2.read()

    # Convert texts to sets of lines
    set1 = set(text1.strip().split('\n'))
    set2 = set(text2.strip().split('\n'))

    # Check if the sets are equal
    if set1 == set2:
        print("The file contents are identical.")
    else:
        print("The file contents are not identical.")