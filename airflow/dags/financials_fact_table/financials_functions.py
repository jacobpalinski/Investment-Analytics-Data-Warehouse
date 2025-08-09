# Import necessary libraries
import json

def polygon_company_financials_extraction_helper(cik: str, fiscal_year: int, fiscal_quarter: str, filing_date: str, financial_statement: str, key: str, item: type, financials_data: list) -> dict:
    """
    Helper function to extract financial data from a JSON response.

    Args:
        financial_statement (str): The type of financial statement (e.g., 'income_statement').
        key (str): The key to extract from the JSON.
        json_path (json): The JSON object containing the financial data.

    Returns:
        dict: A dictionary containing the extracted financial data.
    """
    value = item.value
    currency = item.unit
    if value is not None:
        financials_data.append({'cik': cik, 'fiscal_year': fiscal_year, 'fiscal_quarter': fiscal_quarter, 'filing_date': filing_date, 'financial_statement': financial_statement, 'item': key, 'currency': currency, 'value': value})

def polygon_parse_response(response: dict, cik: str, financials_data: list) -> None:
    """
    Parses the financial data from Polygon API financials endpoint response.

    Args:
        response (dict): The API response containing financial data.
        cik (str): The CIK of the company.

    Returns:
        list: A list of tuples containing the parsed financial data.
    """
    
    for result in response:
        fiscal_year = result.fiscal_year
        fiscal_quarter = result.fiscal_period
        filing_date = result.filing_date

        # Process the financials data
        # Income Statement
        income_statement = result.financials.income_statement
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='revenues',
            item=income_statement.revenues,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='gross_profit',
            item=income_statement.gross_profit,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='operating_income',
            item=income_statement.operating_income_loss,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='net_income',
            item=income_statement.net_income_loss,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='basic_earnings_per_share',
            item=income_statement.basic_earnings_per_share,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='diluted_earnings_per_share',
            item=income_statement.diluted_earnings_per_share,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='operating_expenses',
            item=income_statement.operating_expenses,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='income_statement',
            key='income_tax_benefit',
            item=income_statement.income_tax_expense_benefit,
            financials_data=financials_data
        )
        # Balance Sheet
        balance_sheet = result.financials.balance_sheet
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='balance_sheet',
            key='assets',
            item=balance_sheet.assets,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='balance_sheet',
            key='liabilities',
            item=balance_sheet.liabilities,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='balance_sheet',
            key='equity',
            item=balance_sheet.equity,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='balance_sheet',
            key='current_assets',
            item=balance_sheet.current_assets,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='balance_sheet',
            key='current_liabilities',
            item=balance_sheet.current_liabilities,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='balance_sheet',
            key='noncurrent_liabilities',
            item=balance_sheet.noncurrent_liabilities,
            financials_data=financials_data
        )
        # Cash Flow Statement
        cash_flow_statement = result.financials.cash_flow_statement
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='cash_flow_statement',
            key='net_cash_flow_from_operating_activities',
            item=cash_flow_statement.net_cash_flow_from_operating_activities,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='cash_flow_statement',
            key='net_cash_flow_from_investing_activities',
            item=cash_flow_statement.net_cash_flow_from_investing_activities,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='cash_flow_statement',
            key='net_cash_flow_from_financing_activities',
            item=cash_flow_statement.net_cash_flow_from_financing_activities,
            financials_data=financials_data
        )
        polygon_company_financials_extraction_helper(
            cik=cik,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            filing_date = filing_date,
            financial_statement='cash_flow_statement',
            key='net_cash_flow',
            item=cash_flow_statement.net_cash_flow,
            financials_data=financials_data
        )

def parse_response_sec_api(response: dict, cik: str, financials_data: list) -> None:
    """
    Parses the financial data from SEC API reponse
    
    Args:
        response (dict): The API response containing financial data.
        cik (str): The CIK of the company
    
    Returns:
        list: A list of dictionaries containing the parsed financial data.
    """

    # Dict with keys to filter for in response
    filter_keys = {
    'Revenues': 'income_statement', 'GrossProfit': 'income_statement', 'OperatingIncomeLoss': 'income_statement', 'NetIncomeLoss': 'income_statement', 
    'EarningsPerShareBasic': 'income_statement', 'EarningsPerShareDiluted': 'income_statement', 'OperatingExpenses': 'income_statement', 
    'IncomeTaxExpenseBenefit': 'income_statement', 'Assets': 'balance_sheet', 'Liabilities': 'balance_sheet', 'StockholdersEquity': 'balance_sheet', 
    'AssetsCurrent': 'balance_sheet', 'LiabilitiesCurrent': 'balance_sheet', 'LiabilitiesNoncurrent': 'balance_sheet', 
    'NetCashProvidedByUsedInOperatingActivities': 'cash_flow_statement', 'NetCashProvidedByUsedInInvestingActivities': 'cash_flow_statement', 
    'NetCashProvidedByUsedInFinancingActivities': 'cash_flow_statement'}
    
    # Iterate through relevant filter keys in the response
    for key, value in filter_keys.items():
        if not response.get(key):
            continue
        
        # Only retrieve first currency in the response
        first_currency = next(iter(response['units']))
        last_record = response['units'][first_currency][key][-1]
        
        # Retrieve key information
        cik = cik
        fiscal_year = last_record.get('fy')
        fiscal_quarter = last_record.get('fp')
        filing_date = last_record.get('filed')
        financial_statement = value
        item = key
        currency = first_currency
        value = last_record.get('val')
        
        financials_data.append({'cik': cik, 'fiscal_year': fiscal_year, 'fiscal_quarter': fiscal_quarter, 'filing_date': filing_date, 'financial_statement': financial_statement, 'item': key, 'currency': currency, 'value': value})









        

    
