import os

def generate_sql(tablename, items, filename):
    dir_path = 'sql' + os.sep
    file = open(dir_path + filename + '.sql', 'w')
    for item in items:
        sql = list()
        keys = ['`%s`' % k for k in item.keys()]
        values = ['\'%s\'' % v for v in item.values()]

        sql.append("INSERT INTO `%s` (" % tablename)
        sql.append(", ".join(keys))
        sql.append(") VALUES (")
        sql.append(", ".join(values))
        sql.append(");")

        file.write("".join(sql))
        file.write("\n")
    file.close()

class MySqlConverter():
    def __init__(self):
        # constants (maybe)
        self.v_cols = ['brand', 'model', 'year']
        self.e_cols = ['engine', 'fuel']
        self.ecu_cols = ['model', 'version']

        self.vehicles = []
        self.engines = []
        self.ecus = []

    def parse_item(self, item, cols):
        item_dict = {}

        # assuming same index in cols and item
        for idx, col in enumerate(cols):
            item_dict[col] = item[idx]

        return item_dict

    def add_vehicles(self, vehicles):
        for vehicle in vehicles: 
            self.vehicles.append(self.parse_item(vehicle, self.v_cols))

    def add_engines(self, engines):
        for engine in engines:
            self.engines.append(self.parse_item(engine, self.e_cols))

    def add_ecus(self, ecus):
        for ecu in ecus:
            self.engines.append(self.parse_item(ecu, self.ecu_cols))
    
    def generate_sql_for(self, name):
        if name != 'all':
            items = getattr(self, name)
            generate_sql(name, items, name)
        else:
            generate_sql('vehicles', self.vehicles, 'vehicles')
            generate_sql('engines', self.engines, 'engines')
            generate_sql('ecu', self.engines, 'ecus')

    def print_results(self):
        print("** Vehicles **")
        for vehicle in self.vehicles:
            print(vehicle)
        print("\n")

        print("** Engines **")
        for engine in self.engines:
            print(engine)
        print("\n")

        print("** ECUs **")
        for ecu in self.ecus:
            print(ecu)
        print("\n")