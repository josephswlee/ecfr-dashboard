from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DatabaseLoader

from config.config import Config

import logging

class ETLPipeline:
    def __init__(self, user_params):
        self.admin_config = Config('admin')
        self.versioner_config = Config('versioner', user_params)
        
        self.admin_extractor = DataExtractor(self.admin_config.params)
        self.admin_transformer = DataTransformer(self.admin_config.params)

        self.versioner_extractor = DataExtractor(self.versioner_config.params)
        self.versioner_transformer = DataTransformer(self.versioner_config.params)

        self.db_loader = DatabaseLoader(db_path='data/cfr.db')

    def run_pipeline(self):
        logging.info("Running admin pipeline...")
        admin_raw_data = self.admin_extractor.extract_data()
        admin_transformed = self.admin_transformer.transform_proxy(admin_raw_data)

        logging.info("Loading admin data into database...")
        self.load_admin_data(admin_transformed)

        logging.info("Running versioner pipeline...")
        versioner_raw_data = self.versioner_extractor.extract_data()
        versioner_transformed = self.versioner_transformer.transform_proxy(versioner_raw_data)

        logging.info("Loading versioner data into database...")
        self.load_versioner_data(versioner_transformed)


    def load_admin_data(self, transformed_admin_data):
        for agency in transformed_admin_data:
            self.db_loader.insert_agency(agency)

            agency_id = agency['agency_id']
            cfr_refs = agency.get('cfr_references', [])

            for ref in cfr_refs:
                self.db_loader.insert_cfr_reference(agency_id, ref)

    def load_versioner_data(self, transformed_versioner_data):
        for section in transformed_versioner_data:
            self.db_loader.insert_cfr_section(section)
