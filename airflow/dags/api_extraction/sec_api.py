# Import modules
import requests
import json
import logging
from datetime import datetime, timedelta
from fredapi import Fred

# Create setup for logging
logger = logging.getLogger(__name__)

class SecApi:
    """
    Class for interacting with SEC API
    
    Attributes:
        user_agent (str): User agent used for SEC API request
        base_url (str): Base URL for SEC API request
        filter_keys (dict): Dictionary with relevant financial statement items to extract from SEC API response

    Methods:
        sec_data_request(cik):
            Submits requests to SEC API for a given CIK
        extract_financial_data(cik, response):
            Extracts financials data from SEC API response for a given CIK

    """
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self.base_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK"
        self.filter_keys = {'Revenues': ['revenues', 'income_statement'], 'GrossProfit': ['gross_profit', 'income_statement'], 'OperatingIncomeLoss': ['operating_income', 'income_statement'], 
        'NetIncomeLoss': ['net_income', 'income_statement'], 
        'EarningsPerShareBasic': ['basic_earnings_per_share','income_statement'], 'EarningsPerShareDiluted': ['diluted_earnings_per_share', 'income_statement'], 
        'OperatingExpenses': ['operating_expenses', 'income_statement'], 'IncomeTaxExpenseBenefit': ['income_tax_benefit', 'income_statement'], 
        'Assets': ['assets','balance_sheet'], 'Liabilities': ['liabilities','balance_sheet'], 'StockholdersEquity': ['equity','balance_sheet'], 
        'AssetsCurrent': ['current_assets', 'balance_sheet'] , 'LiabilitiesCurrent': ['current_liabilities', 'balance_sheet'], 
        'LiabilitiesNoncurrent': ['noncurrent_liabilities','balance_sheet'], 
        'NetCashProvidedByUsedInOperatingActivities': ['net_cash_flow_from_operating_activities', 'cash_flow_statement'], 
        'NetCashProvidedByUsedInInvestingActivities': ['net_cash_flow_from_investing_activities', 'cash_flow_statement'], 
        'NetCashProvidedByUsedInFinancingActivities': ['net_cash_flow_from_financing_activities', 'cash_flow_statement']}
    
    def sec_data_request(self, cik: str) -> dict:
        """
        Submits requests to SEC API for a given CIK

        Args:
            cik (str): CIK data is being extracted for
        
        Returns:
            dict: Dictionary with SEC API response data
        """
        # Create headers for SEC API requests
        sec_api_headers = {'User-Agent': self.user_agent}
        url = self.base_url + f"{cik}.json"

        try:
            response = requests.get(url, headers=sec_api_headers, timeout=10)
            return response
        except Exception as e:
            logger.exception(f"SEC API error for {cik}: {e}")
            return {}
        
    def extract_financial_data(self, cik: str, response) -> dict:
        """
        Extracts financials data from SEC API response for a given CIK

        Args:
            cik (str): CIK data is being extracted for
            response: Dictionary / JSON with SEC API response data
        
        Returns:
            list: List containing extracted financial data
        """
        if isinstance(response, dict):
            response_dict = response
        else:
            response_dict = response.json()
        
        facts = response_dict.get('facts', {})
        data = facts.get('us-gaap') or facts.get('ifrs-full', {})

        results = []

        # Iterate through relevant filter keys in the response
        for key, values in self.filter_keys.items():
            key_data = data.get(key)
            if not key_data:
                continue

            units = key_data.get('units')
            if not units:
                continue
            
            # Only retrieve first currency in the response
            first_currency = next(iter(units))
            if not units[first_currency]:
                continue
            
            last_record = units[first_currency][-1]
            
            # Retrieve key information
            cik = cik
            fiscal_year = last_record.get('fy')
            fiscal_quarter = last_record.get('fp')
            filing_date = last_record.get('filed')
            financial_statement = values[1]
            item = values[0]
            currency = first_currency
            value = last_record.get('val')
            
            results.append({
            'cik': cik,
            'fiscal_year': last_record.get('fy'),
            'fiscal_quarter': last_record.get('fp'),
            'filing_date': last_record.get('filed'),
            'financial_statement': values[1],
            'item': values[0],
            'currency': first_currency,
            'value': last_record.get('val')
            })
        
        return results





