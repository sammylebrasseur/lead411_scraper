import requests
import time
from datetime import datetime
from itertools import islice
import pandas as pd
import os

# SETUP LOOM: https://www.loom.com/share/818a9ac136d8492b9ae034c07d9a8f01

# 1. VARIABLES
output_file_name = 'lead411_contacts.csv'
START_PAGE = 1
PER_PAGE = 10000

# 2. REQUEST INFO
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL3dlYmFwaS5sZWFkNDExLmNvbS9hcGkvdjIvY3VzdG9tZXIvY3JlYXRlX2ZyZWVfdHJpYWwiLCJpYXQiOjE3NTM0ODg4MDMsImV4cCI6MTc1MzY2MTYwMywibmJmIjoxNzUzNDg4ODAzLCJqdGkiOiJORTE4blBpVDJGS0JRdTFTIiwic3ViIjoiNDk0ODMxNyIsInBydiI6ImRhOWM1NTYwZGUxNjE1M2JhODE4MGM1ODkzZmQ3NDk0YWQ1OGFmOWUiLCJDS1k9IjoiVU9FKzRNNVhxRWJvM0N3MSIsIkZLVnB2S2NPNFJLMGhrUnFIRTRhIjoiTExsMnA1UUQ1MWp2M0NzaVFIY0xVYXBvUzR3WWRURTBrZGF0Y0hjWDNFRUVkSk1UaWp6R29OeTRDVExzekw1amhtS2twak9Ra0ZnV2hCN1lRSHRmK0ZIQmM2Tm1vSDllSEVOWDQ4dmM1ZGc0K25kY2FhWlhhemFIKy85YkJXRlBIMWJYY2phVWZ2cHNveGRwbW1LWmx6c09TRnVpIn0.53CNIf0lWEirYI3grnRDDHPO42kaO83-2UljVWa2Od4',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://web.lead411.com',
    'priority': 'u=1, i',
    'referer': 'https://web.lead411.com/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

params = {
    'page': '1',
    'per_page': '25',
}

data = {
    'crypt': '{"ct":"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","iv":"3f71e7fc45162b3c7eaeadf0a9754726","s":"17b98ba88feefd7f"}',
}

def remove_duplicate_dicts_from_list_of_dicts_based_on_key(key, list_of_dicts):
    seen = set()
    unique_list = []
    duplicates = []

    for entry in list_of_dicts:
        value = entry.get(key)
        try:
            if value not in seen:
                unique_list.append(entry)
                seen.add(value)
            else:
                duplicates.append(entry[key])
        except:
            pass

    return unique_list, duplicates

page = START_PAGE

while True:

    params['page'] = str(page)
    params['per_page'] = str(PER_PAGE)


    print(f'\nRequesting page {page}...')

    start_time = time.time()
    response = requests.post('https://webapi.lead411.com/api/v2/searchUsingJSON', params=params, headers=headers, data=data)
    request_time = time.time() - start_time

    results = response.json().get('AllResults')

    contacts = []

    if not results:
        print(f'\n{response.status_code} - {response.text}\nNo results found on page {page}. Exiting.')
        exit()

    for result in results:
        contact = {}

        # COMPANY DATA
        company_meta = result.get('json_meta').get('company_meta')
        contact['l4_company_id'] = result['company_id']
        contact['l4_company_id'] = result['company_id']

        if company_meta:
            try:
                contact['l4_location'] = company_meta['locality']
            except:
                pass
            contact['l4'] = True
            contact['latest_source'] = 'l4'
            try:
                contact['l4_industry'] = company_meta['industry']
            except:
                pass
            try:
                contact['l4_sic_definitions'] = company_meta['sic_definitions']
            except:
                pass
            try:
                contact['company_name'] = company_meta['company_name']
                if contact['company_name']:
                    contact['company_name_clean'] = clean_company_name(contact['company_name'])
            except:
                pass
            try:
                contact['l4_company_yearly_revenues'] = company_meta['yearly_revenues']
            except:
                pass
            try:
                contact['l4_company_number_of_employees'] = company_meta['number_of_employees']
            except:
                pass
            try:
                contact['company_street_address'] = company_meta['address1']
            except:
                pass
            try:
                contact['company_city'] = company_meta['city']
            except:
                pass
            try:
                contact['company_state'] = get_full_state_name(company_meta['region_code'])
            except:
                pass
            try:
                contact['company_country'] = get_full_country_name(input_country=company_meta['company_country'])
            except:
                pass
            try:
                contact['company_postal_code'] = company_meta['zip']
            except:
                pass
            try:
                company_uk_zip = company_meta['uk_zip'].replace("\\","")
                if company_uk_zip != 'N/A':
                    contact['company_uk_zip'] = company_uk_zip
            except:
                pass
            try:
                contact['company_founded_year'] = company_meta['company_founded'] if company_meta['company_founded'] != 'N/A' else None
                if len(str(contact['company_founded_year'])) > 4:
                    abbreviated_founded_year = str(contact['company_founded_year'])[:4]
                    contact['company_founded_year'] = int(abbreviated_founded_year)
            except:
                pass
            try:
                contact['company_phone'] = company_meta['company_phone']
            except:
                pass
            try:
                website_url = company_meta['company_URL']
                if website_url:
                    contact['company_website_url'] = url_clean(website_url.replace("\\",""))
            except:
                pass
            try:
                company_linkedin_url = company_meta['company_linkedin']
                if company_linkedin_url:
                    contact['company_linkedin_url'] = url_clean(company_linkedin_url.replace("\\",""))
            except:
                pass
            try:
                company_twitter_url = company_meta['company_twitter']
                if company_twitter_url:
                    contact['company_twitter_url'] = url_clean(company_twitter_url.replace("\\",""))
            except:
                pass
            try:
                company_facebook_url = company_meta['company_facebook']
                if company_facebook_url:
                    contact['company_facebook_url'] = url_clean(company_facebook_url.replace("\\",""))
            except:
                pass
            try:
                company_instagram_url = company_meta['company_instagram']
                if company_instagram_url:
                    contact['company_instagram_url'] = url_clean(company_instagram_url.replace("\\",""))
            except:
                pass
            try:
                company_youtube_url = company_meta.get('company_youtube')
                if company_youtube_url:
                    contact['company_youtube_url'] = url_clean(company_youtube_url.replace("\\",""))
            except:
                pass
            try:
                company_wikipedia_url = company_meta['company_wikipedia']
                if company_wikipedia_url:
                    contact['company_wikipedia_url'] = url_clean(company_wikipedia_url.replace("\\",""))
            except:
                pass
            try:
                contact['l4_company_lead_score'] = company_meta['company_lead_score']
            except:
                pass
                
            
        # CONTACT DATA
        employee_meta = result.get('json_meta').get('employee_meta')
        if employee_meta:
            contact['l4'] = True
            contact['latest_source'] = 'l4'
            # if state:
            #     contact['state'] = state
            # contact['country'] = country
            # contact['seniority'] = seniority
            try:
                contact['l4_employee_id'] = result['employee_id']
            except:
                pass
            contact_name = employee_meta.get('employee_name')
            if contact_name:
                try:
                    contact['first_name'] = contact_name.split(' ')[0]
                except:
                    pass
                try:
                    contact['last_name'] = contact_name.split(' ')[1]
                except:
                    pass
            try:
                contact['title'] = employee_meta['title']
            except:
                pass
            try:
                contact_last_seen = employee_meta['last_seen']
                if contact_last_seen:
                    contact['last_seen'] = datetime.strptime(contact_last_seen, "%m-%d-%Y").strftime("%Y-%m-%d")
            except:
                pass
            try:
                contact_linkedin_url = employee_meta['employee_linkedin']
                if contact_linkedin_url:
                    contact_linkedin_url = contact_linkedin_url.replace("\\","")
                    contact['linkedin_url'] = url_clean(contact_linkedin_url)
            except:
                pass
            try:
                contact['email'] = employee_meta['email'].lower()
            except:
                pass
            try:
                email_date = employee_meta['email_date']
                if email_date:
                    email_date = email_date.replace("\\","")
                    email_date = email_date.replace("/","-")
                    contact['email_date'] = datetime.strptime(email_date, "%m-%d-%Y").strftime("%Y-%m-%d")
            except:
                pass
            try:
                contact['email_result'] = employee_meta['email_result']
            except:
                pass
            try:
                contact['direct_phone'] = employee_meta['direct_dial']
            except:
                pass
            try:
                contact['direct_phone_type'] = employee_meta['direct_type']
            except:
                pass
            try:
                contact['direct_phone_country_code'] = employee_meta['direct_country_code']
            except:
                pass
            try:
                contact['direct_phone_prefix'] = employee_meta['direct_prefix']
            except:
                pass
            try:
                contact['mobile_phone'] = employee_meta['mobile_dial']
            except:
                pass
            try:
                contact['mobile_phone_type'] = employee_meta['mobile_type']
            except:
                pass
            try:
                contact['mobile_phone_country_code'] = employee_meta['mobile_country_code']
            except:
                pass
            try:
                contact['mobile_phone_prefix'] = employee_meta['mobile_prefix']
            except:
                pass
            if contact:
                contacts.append(contact)

    def batch_iterable(iterable, size):
        """Yield successive chunks from iterable."""
        it = iter(iterable)
        while batch := list(islice(it, size)):
            yield batch

    # WRITE CONTACTS TO CSV
    if contacts:
        # Remove duplicates
        unique_contacts, duplicate_contacts = remove_duplicate_dicts_from_list_of_dicts_based_on_key(key='linkedin_url', list_of_dicts=contacts)
        # Remove values that are empty strings
        unique_cleaned_contacts = []
        for contact in unique_contacts:
            unique_cleaned_contact = {}
            for key, value in contact.items():
                if value and value != '' and value != 'Not Available' and value != 'N/A' and value != 'not available' and value != 'not available' and value != 'n/a' and value != 'na':
                    unique_cleaned_contact[key] = value
            unique_cleaned_contacts.append(unique_cleaned_contact)
        # Append to CSV
        output_file = 'output.csv'
        df = pd.DataFrame(unique_cleaned_contacts)  # wrap your single row dict in a list
        df.to_csv(
            output_file,
            mode='a',
            header=not os.path.isfile(output_file),
            index=False
        )




            
    end_time = time.time()
    print(f"\nPage {page} finished in: {end_time - start_time} seconds.")
    print(f'Request time: {request_time} seconds.')
    print(f'Upsert time: {end_time - start_time - request_time} seconds.')
    page += 1
    