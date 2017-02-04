import os
import csv

from collections import OrderedDict

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
        self.vehicle_engines = []
        self.engine_ecus = []

    def validate_csv(self, file_path):
        try:
            file = open(file_path)
        except PermissionError:
            print("access denied in creating file!")
            return False
        except IOError:
            print("could not read file!")
            return False
        
        reader = csv.reader(file)
        
        for row in reader:
            if len(row) != 8:
                file.close()
                return False
        file.close()
        return True

    def read_csv(self, file_path):
        valid = self.validate_csv(file_path)
        
        if valid:
            try:
                file = open(file_path)
            except PermissionError:
                print("access denied in creating file!")
                return False
            except IOError:
                print("could not read file!")
                return False
            
            reader = csv.reader(file)

            for row in reader:
                self.raw_csv.append(row)
            file.close()
        else:
            self.unparsed_files.append(file_path)

    def add_vehicle(self, brand, model, year):
        if brand != '':
            brand = brand.lower()
            brand = brand.title()

        if year == '':
            year = None

        vehicle = {'brand': brand, 'model': model, 'year': year}

        if vehicle not in self.vehicles:
            self.vehicles.append(vehicle)

        return self.vehicles.index(vehicle)

    def add_engine(self, engine, fuel):
        engine = {'engine': engine, 'fuel': fuel}
        if engine not in self.engines:
            self.engines.append(engine)
        
        return self.engines.index(engine)

    def add_ecu(self, ecu_type, model, version):
        if ecu_type == 'ECU':
            ecu = {'model': model, 'version': version}
            if ecu not in self.ecus:
                self.ecus.append(ecu)
            
            return self.ecus.index(ecu)

    def parse_rows(self):
        raw_data = self.raw_csv
        for (brand, model, engine, fuel, year, ecu, ecu_model, ecu_version) in raw_data:
            vehicle = self.add_vehicle(brand, model, year)
            engine = self.add_engine(engine, fuel)
            ecu = self.add_ecu(ecu, ecu_model, ecu_version)
            
            ve = OrderedDict([('vehicle_id', vehicle), ('engine_id', engine)])
            self.vehicle_engines.append(ve)

            # sometimes engines don't have the ecu
            if(ecu):
                ee = OrderedDict([('engine_id', engine), ('ecu_id', ecu)])
                self.engine_ecus.append(ee)
            

    def read(self, file):
        file_path = self.path + os.sep + file
        self.read_csv(file_path)
        self.parse_rows()
