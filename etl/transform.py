from typing import Dict, Any
from lxml import etree
import re
import logging

class ServiceNotImplementedError(Exception):
    """
    Custom exception raised when an API service type is not implemented.
    """
    def __init__(self, service_type: str, message: str = "API service type not implemented"):
        self.service_type = service_type
        # Call the base Exception class's constructor with the message
        super().__init__(f"{message}: '{service_type}'")

class DataTransformer:
    def __init__(self, params = Dict):
        self.params = params

    def clean_text(self, text):
        if text:
            # check whitespaces and strip
            return re.sub(r'\s+', ' ', text).strip()
        return ''
    
    def transform_proxy(self, api_response):

        if self.params['service'] == 'admin':
            return self.transform_admin_api(api_response)
        elif self.params['service'] == 'versioner':
            return self.transform_versioner_api(api_response)
        else:
            raise ServiceNotImplementedError(self.params['service'])
        
    def transform_admin_api(self, api_response):

        if self.params['service'] != 'admin':
            raise Exception('Wrong Service Called')
        
        agencies = api_response.get('agencies', [])
        rows = []
        # track parent-child mapping
        slug_to_id = {} 
        # handling auto increment of the PK
        next_id = 1

        for agency in agencies:
            parent_id = None
            agency_slug = self.clean_text(agency['slug'])

            agency_cfr_refs = [
                {
                    'title': ref.get('title'),
                    'chapter': ref.get('chapter'),
                    'part': ref.get('part')
                }
                for ref in agency.get('cfr_references', [])
            ]

            row = {
                'agency_id': next_id,
                'name': self.clean_text(agency['name']),
                'short_name': self.clean_text(agency.get('short_name')),
                'display_name': self.clean_text(agency.get('display_name')),
                'sortable_name': self.clean_text(agency.get('sortable_name')),
                'slug': agency_slug,
                'parent_id': parent_id,
                'cfr_references': agency_cfr_refs
            }
            rows.append(row)
            slug_to_id[agency_slug] = next_id
            next_id += 1
            
            # children process
            for child in agency.get('children', []):
                child_slug = self.clean_text(child['slug'])

                child_cfr_refs = [
                    {
                        'title': ref.get('title'),
                        'chapter': ref.get('chapter'),
                        'part': ref.get('part')
                    }
                    for ref in child.get('cfr_references', [])
                ]

                child_row = {
                    'agency_id': next_id,
                    'name': self.clean_text(child['name']),
                    'short_name': self.clean_text(child.get('short_name')),
                    'display_name': self.clean_text(child.get('display_name')),
                    'sortable_name': self.clean_text(child.get('sortable_name')),
                    'slug': child_slug,
                    'parent_id': slug_to_id[agency_slug],  # parent points to main agency
                    'cfr_references': child_cfr_refs
                }
                rows.append(child_row)
                slug_to_id[child_slug] = next_id
                next_id += 1
        
        return rows

    def transform_versioner_api(self, api_response):
        if self.params['service'] != 'versioner':
            raise Exception('Wrong Service Called')
        
        
        root = etree.fromstring(api_response.encode('utf-8'))
        cfr_data = []

        for div1 in root.findall('.//DIV1'):  # TITLE
            title_number = div1.get('N')
            title_head = (div1.findtext('HEAD') or '').strip()

            for div3 in div1.findall('.//DIV3'):  # CHAPTER
                chapter_number = div3.get('N')
                chapter_head = (div3.findtext('HEAD') or '').strip()

                for div4 in div3.findall('.//DIV4'):  # SUBCHAPTER
                    subchapter_number = div4.get('N')
                    subchapter_head = (div4.findtext('HEAD') or '').strip()

                    for div5 in div4.findall('.//DIV5'):  # PART
                        part_number = div5.get('N')
                        part_head = (div5.findtext('HEAD') or '').strip()

                        for div8 in div5.findall('.//DIV8'):
                            section_number = div8.get('N')
                            section_title = (div8.findtext('HEAD') or '').strip()
                            body = ' '.join(' '.join(p.itertext()).strip() for p in div8.findall('.//P'))

                            cfr_data.append({
                                'title_number': title_number,
                                'title_head': title_head,
                                'chapter_number': chapter_number,
                                'chapter_head': chapter_head,
                                'subchapter_number': subchapter_number,
                                'subchapter_head': subchapter_head,
                                'part_number': part_number,
                                'part_head': part_head,
                                'section_number': section_number,
                                'section_title': section_title,
                                'body': body
                            })

        return cfr_data


    