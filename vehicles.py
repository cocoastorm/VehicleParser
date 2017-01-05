import os
import pymongo
import configparser

# Convert Dictionaries to MySQL
class MySqlConverter():
    def __init__(self):
        self.ve_cols = ['vehicle_id', 'engine_id']
        self.ee_cols = ['engine_id', 'ecu_id']
        
        self.vehicles = []
        self.engines = []
        self.ecus = []
        
        self.vehicle_engines = []
        self.engine_ecus = []

    def parse_item(self, item, cols):
        item_dict = {}

        # assuming same index in cols and item
        for idx, col in enumerate(cols):
            item_dict[col] = item[idx]

        return item_dict

    def add_vehicles(self, vehicles):
        self.vehicles = vehicles

    def add_engines(self, engines):
        self.engines = engines

    def add_ecus(self, ecus):
        self.ecus = ecus

    def add_vehicle_engines(self, vehicle_engines):
        for vehicle_engine in vehicle_engines:
            self.vehicle_engines.append(self.parse_item(vehicle_engine, self.ve_cols))

    def add_engine_ecus(self, engine_ecus):
        for engine_ecu in engine_ecus:
            self.engine_ecus.append(self.parse_item(engine_ecu, self.ee_cols))
    
    def generate_sql_for(self, name):
        if name != 'all':
            items = getattr(self, name)
            generate_sql(name, name, items)
        else:
            generate_sql('vehicles', 'vehicles', self.vehicles)
            generate_sql('engines', 'engines', self.engines)
            generate_sql('ecu', 'ecus', self.ecus)
            generate_sql_relationship('VehicleEngine', 'VehicleEngine', self.vehicle_engines)
            generate_sql_relationship('EngineEcu', 'EngineEcu', self.engine_ecus)

    def generate_sql_vehicle_engine(self):
        generate_sql_relationship('VehicleEngine', 'VehicleEngine', self.vehicle_engines)

    def generate_sql_engine_ecu(self):
        generate_sql_relationship('EngineEcu', 'EngineEcu', self.engine_ecus)

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

        print("** Vehicle Engines **")
        for vehicle_engine in self.vehicle_engines:
            print(vehicle_engine)
        print("\n")

        print("** Engine ECUs **")
        for engine_ecu in self.engine_ecus:
            print(engine_ecu)
        print("\n")

def generate_sql(tablename, filename, items):
    dir_path = 'sql' + os.sep
    
    try:
        file = open(dir_path + filename + '.sql', 'w')
    except PermissionError:
        print("access denied in creating file!")
        return False
    except IOError:
        print("could not create file!")
        return False
    
    for idx, item in enumerate(items):
        sql = list()
        keys = ['`%s`' % k for k in item.keys()]
        values = ['\'%s\'' % v for v in item.values()]
        primary_index = str(idx + 1) + ', ' 

        sql.append("INSERT INTO `%s` (" % tablename)
        sql.append(", ".join(keys))
        sql.append(") VALUES (")
        sql.append(primary_index)
        sql.append(", ".join(values))
        sql.append(");")

        file.write("".join(sql))
        file.write("\n")
    file.close()

def generate_sql_relationship(tablename, filename, items):
    dir_path = 'sql' + os.sep
    
    try:
        file = open(dir_path + filename + '.sql', 'w')

    except PermissionError:
        print("access denied in creating file!")
        return False

    except IOError:
        print("could not create file!")
        return False

    for idx, item in enumerate(items):
        sql = list()
        keys = ['`%s`' % k for k in item.keys()]
        values = ['%s' % str(v + 1) if v is not None else '' for v in item.values()]
        primary_index = str(idx + 1) + ', ' 

        sql.append("INSERT INTO `%s` (" % tablename)
        sql.append(", ".join(keys))
        sql.append(") VALUES (")
        sql.append(primary_index)
        sql.append(", ".join(values))
        sql.append(");")

        file.write("".join(sql))
        file.write("\n")
    file.close()

# Transfer Data to MongoDB
class bettermongo():
    def __init__(self):
        self.vehicles = []
        self.engines = []
        self.ecus = []
        
        self.vehicle_engines = []
        self.engine_ecus = []

        self.read_config()
        self.create_mongo()

    def read_config(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        if config['Mongo']:
            self.mongohost = config['Mongo']['Host']
            self.mongoport = int(config['Mongo']['Port'])
            self.mongodb = config['Mongo']['Database']

    def create_mongo(self):
        self.mongoclient = pymongo.MongoClient(self.mongohost, self.mongoport)
        self.db = self.mongoclient[self.mongodb]

    def add_vehicles(self, vehicles):
        self.vehicles = vehicles

    def add_engines(self, engines):
        self.engines = engines

    def add_ecus(self, ecus):
        self.ecus = ecus

    def add_vehicle_engines(self, vehicle_engines):
        self.vehicle_engines = vehicle_engines

    def add_engine_ecus(self, engine_ecus):
        self.engine_ecus = engine_ecus

    def print_items(self, items):
        for item in items:
            print(item)
            print("\n")

    def reformat(self):
        vehicles = list()

        for vehicle_idx, engine_idx in self.vehicle_engines:
            vehicle = self.vehicles[int(vehicle_idx)]
            engine = self.engines[int(engine_idx)]

            if vehicle not in vehicles:
                vehicle['engines'] = list()
                vehicle['engines'].append(engine)
                vehicles.append(vehicle)
            else:
                vehicle = vehicles[vehicles.index(vehicle)]
                vehicle['engines'].append(engine)
        
        self.print_items(vehicles)
        return vehicles

    def add_mongo(self):
        vehicles = self.reformat()
        result = self.db.vehicles.insert_many(vehicles)

        print(result)
