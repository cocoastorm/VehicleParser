# standard library
import os

# my own ghetto
import bettercsv
import vehicles

# Display a title bar.
def display_title():
    print("**********************************************")
    print("*** VehiclesParser - CSV - Objects - MySQL ***")
    print("**********************************************")

def display_options():
    print("\n1) List available csv files to parse")
    print("2) Add csv file to parse")
    print("3) Convert current data to MySQL")
    print("\n\nType 'quit' to exit this application\n")

def list_directory(path):
    for (dirpath, _, filenames) in os.walk(path):
        for filename in filenames:
            yield filename

def list_csv_files():
    path = './csv'
    files = list_directory(path)
    
    print("**********************************************")
    print("***************** CSV Files ******************")
    print("**********************************************")
    
    for file in files:
        print(file)

def parse_csv_file(parser_obj):
    file = input("Enter the name of the csv file you wish to parse (all): ")
    
    if file.lower() != 'all':
        parser_obj.read(file)
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

# Global Variables
csv_path = './csv'
choice = ''

csv_parser = bettercsv.Parser(csv_path)

# display header title
display_title()

# Main Loop
while choice != 'quit':
    display_options()
    choice = input("Your Option: ")
    choice = choice.lower()
    os.system('clear')

    if choice == '1':
        list_csv_files()
    elif choice == '2':
        parse_csv_file(csv_parser)
    elif choice == '3':
        convert_data_mysql(csv_parser)
