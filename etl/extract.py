import requests
import json
from typing import Dict, Any
import logging

class ServiceNotImplementedError(Exception):
    """
    Custom exception raised when an API service type is not implemented.
    """
    def __init__(self, service_type: str, message: str = "API service type not implemented"):
        self.service_type = service_type
        # Call the base Exception class's constructor with the message
        super().__init__(f"{message}: '{service_type}'")

class DataExtractor:
    def __init__(self, params = Dict):
        self.base_url = params.get('base_url', None)
        self.endpoint = params.get('endpoint', None)
        self.params = params

    def extract_data(self):
        """
        Extract data from public API
        """
        
        if not self.params:
            raise ValueError("The 'params' parameter cannot be null or empty.")
        if 'service' not in self.params:
            raise ValueError("Missing 'service' key in parameters.")

        if self.params['service'] == 'admin':
            try: 
                response = requests.get(f'{self.base_url}/{self.endpoint}')
                response.raise_for_status()
                retrieved_data = response.json()
            except requests.exceptions.RequestException as e:
                print(f'Error fetching Admin data: {e}')

            return retrieved_data

        elif self.params['service'] == 'versioner':
            try:
                date = self.params['date']
                title = self.params['title']
                xml_endpoint = f'api/versioner/v1/full/{date}/title-{title}.xml'
                response = requests.get(f'{self.base_url}/{xml_endpoint}')
                response.raise_for_status()
                xml_content = response.text
            except requests.exceptions.RequestException as e:
                print(f'Error fetching XML data: {e}')
            
            return xml_content
        else:
            raise ServiceNotImplementedError(self.params['service'])