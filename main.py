# standard library
import os

# my own ghetto modules
import bettercsv
import vehicles

# Display a title bar.
def display_title():
    print("**********************************************")
    print("*************** Vehicles Parser **************")
    print("**********************************************")

# Display the main options
def display_options():
    print("1) Add CSV file(s) to Parse")
    print("2) Convert Current Data to MySQL")
    print("\n\nType 'quit' to exit this application\n")

# List the files in a specified directory (path)
def list_directory(path):
    for (dirpath, _, filenames) in os.walk(path):
        for filename in filenames:
            yield filename

# Get the current CSV Files
def csv_files():
    files = list_directory('.' + os.sep + 'csv')
    return files

def display_csv_files(files):
    if len(files) <= 0:
        print("No CSV Files found")
    else:
        print("CSV Files Found: \n")
    
        for idx, file in enumerate(files):
            print('%d. %s' % (idx, file))

def parse_csv_file(parser_obj):
    ffs = list(csv_files)

    # Display CSV Files
    display_csv_files(ffs)
    
    input_file = input("Enter the CSV filename or number you wish to parse (ALL): ")
    
    if is_number(input_file):
        try:
            file = ffs[int(input_file)]
            parser_obj.read(file)
        except IndexError:
            input_file = ''

    elif input_file.lower() != '' and not is_number(input_file):
        parser_obj.read(input_file)
    else:
        for file in list_directory('.' + os.sep + 'csv'):
            parser_obj.read(file)

    # print out results for now
    print("** Vehicles **")
    for vehicle in csv_parser.vehicles:
        print(vehicle)
    print("\n")
    
    print("** Engines **")
    for engine in csv_parser.engines:
        print(engine)
    print("\n")

    print("** ECUs **")
    for ecu in csv_parser.ecus:
        print(ecu)
    print("\n")

    # invalid csv files
    if len(parser_obj.unparsed_files) > 0:
        print("** Unparsed CSV Files **")
        for bad in parser_obj.unparsed_files:
            print(bad)
        print("\n")

def print_convert_mysql_options():
    print("\nPlease select which data you want to dump:")
    print("\n1) Vehicles")
    print("2) Engines")
    print("3) ECUs")
    print("4) ALL")
    print("\n\n")

def convert_data_mysql(parser_obj):
    con = vehicles.MySqlConverter()
    con.add_vehicles(parser_obj.vehicles)
    con.add_engines(parser_obj.engines)
    con.add_ecus(parser_obj.ecus)

    con.print_results()

    print_convert_mysql_options()
    dump_option = input("Your Option: ")

    if dump_option == 1:
        con.generate_sql_for('vehicles')
    elif dump_option == 2:
        con.generate_sql_for('engines')
    elif dump_option == 3:
        con.generate_sql_for('ecus')
    else:
        con.generate_sql_for('all')

def is_number(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

# Global Variables
csv_path = './csv'
choice = ''

csv_parser = bettercsv.Parser(csv_path)
csv_files = csv_files()

# Display Title
display_title()

# Main Loop
while choice != 'quit':
    display_options()
    choice = input("Your Option: ")
    choice = choice.lower()
    os.system('clear')
    
    # GOTO Option
    if choice == '1':
        parse_csv_file(csv_parser)
    elif choice == '2':
        convert_data_mysql(csv_parser)
