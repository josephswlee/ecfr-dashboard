import os

class Config:
    
    def __init__(self, service: str, user_params: dict = None):
        self.BASE_URL = "https://www.ecfr.gov/"
        self.service = service
        self.user_params = user_params or {}
        self.params = self.paramBuilder(service, self.user_params)
        

    def paramBuilder(self, service: str, user_params: dict):

        params = {'service': service}

        if service == 'admin':
            params['base_url'] = self.BASE_URL
            params['endpoint'] = 'api/admin/v1/agencies.json'

        elif service == 'versioner':
            params['base_url'] = self.BASE_URL
            params['date'] = user_params['date']
            params['title'] = user_params['title']

        else:
            ValueError(f'Unsupported service type: {service}')

        return params