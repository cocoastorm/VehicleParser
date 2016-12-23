import os
import csv

class Parser(object):
    def __init__(self, path):
        self.path = path
        self.raw_csv = []
        self.unparsed_files = []
        
        # lists
        self.vehicles = []
        self.engines = []
        self.ecus = []

        # related data
        self.vehicle_engine = []
        self.engine_ecu = []

    def validate_csv(self, file_path):
        reader = csv.reader(open(file_path))
        for row in reader:
            if len(row) != 8:
                return False
        return True

    def read_csv(self, file_path):
        valid = self.validate_csv(file_path)
        
        if valid:
            file = open(file_path)
            reader = csv.reader(file)

            for row in reader:
                self.raw_csv.append(row)
        else:
            self.unparsed_files.append(file_path)

    def add_vehicle(self, brand, model, year):
        if brand != '':
            brand = brand.lower()
            brand = brand.title()

        if year == '':
            year = None

        vehicle = [brand, model, year]

        if vehicle not in self.vehicles:
            self.vehicles.append(vehicle)

        return self.vehicles.index(vehicle)

    def add_engine(self, engine, fuel):
        engine = [engine, fuel]
        if engine not in self.engines:
            self.engines.append(engine)
        
        return self.engines.index(engine)

    def add_ecu(self, ecu_type, model, version):
        if ecu_type == 'ECU':
            ecu = [model, version]
            if ecu not in self.ecus:
                self.ecus.append(ecu)
            
            return self.ecus.index(ecu)

    def parse_rows(self):
        raw_data = self.raw_csv
        for (brand, model, engine, fuel, year, ecu, ecu_model, ecu_version) in raw_data:
            vehicle = self.add_vehicle(brand, model, year)
            engine = self.add_engine(engine, fuel)
            ecu = self.add_ecu(ecu, ecu_model, ecu_version)
            
            self.vehicle_engine.append([vehicle, engine])
            self.engine_ecu.append([engine, ecu])

    def read(self, file):
        file_path = self.path + os.sep + file
        self.read_csv(file_path)
        self.parse_rows()
