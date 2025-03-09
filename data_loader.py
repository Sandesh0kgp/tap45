import pandas as pd
import logging
from typing import Optional, Dict, List, Tuple
import json

class DataLoader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.bond_details = None
        self.cashflow_details = None
        self.company_insights = None

    def load_bond_data(self, df: pd.DataFrame) -> None:
        """Load and process bond data"""
        try:
            required_columns = ['isin', 'company_name', 'issue_size', 'allotment_date', 
                              'maturity_date', 'issuer_details', 'instrument_details', 
                              'coupon_details']
            
            # Validate required columns
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

            # Parse JSON columns
            json_columns = ['issuer_details', 'instrument_details', 'coupon_details', 
                          'redemption_details', 'credit_rating_details', 'listing_details']
            
            for col in json_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: 
                        json.loads(x) if isinstance(x, str) and x.strip() else {})

            self.bond_details = df
            self.logger.info(f"Loaded {len(df)} bond records")
        except Exception as e:
            self.logger.error(f"Error loading bond data: {str(e)}")
            raise

    def load_cashflow_data(self, df: pd.DataFrame) -> None:
        """Load and process cashflow data"""
        try:
            required_columns = ['isin', 'cash_flow_date', 'cash_flow_amount']
            
            # Validate required columns
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

            # Convert date columns
            date_columns = ['cash_flow_date', 'record_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')

            # Convert numeric columns
            numeric_columns = ['cash_flow_amount', 'principal_amount', 'interest_amount', 
                             'tds_amount', 'remaining_principal']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            self.cashflow_details = df
            self.logger.info(f"Loaded {len(df)} cashflow records")
        except Exception as e:
            self.logger.error(f"Error loading cashflow data: {str(e)}")
            raise

    def load_company_data(self, df: pd.DataFrame) -> None:
        """Load and process company data"""
        try:
            required_columns = ['company_name', 'company_industry']
            
            # Validate required columns
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

            # Parse JSON columns
            json_columns = ['key_metrics', 'income_statement', 'balance_sheet', 
                          'cashflow', 'lenders_profile']
            
            for col in json_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: 
                        json.loads(x) if isinstance(x, str) and x.strip() else {})

            self.company_insights = df
            self.logger.info(f"Loaded {len(df)} company records")
        except Exception as e:
            self.logger.error(f"Error loading company data: {str(e)}")
            raise

    def get_bond_details(self, isin: Optional[str] = None) -> pd.DataFrame:
        """Retrieve bond details, optionally filtered by ISIN"""
        if self.bond_details is None:
            raise ValueError("Bond data not loaded")
        if isin:
            return self.bond_details[self.bond_details['isin'] == isin]
        return self.bond_details

    def get_cashflow_details(self, isin: Optional[str] = None) -> pd.DataFrame:
        """Retrieve cashflow details, optionally filtered by ISIN"""
        if self.cashflow_details is None:
            raise ValueError("Cashflow data not loaded")
        if isin:
            return self.cashflow_details[self.cashflow_details['isin'] == isin]
        return self.cashflow_details

    def get_company_insights(self, company_name: Optional[str] = None) -> pd.DataFrame:
        """Retrieve company insights, optionally filtered by company name"""
        if self.company_insights is None:
            raise ValueError("Company data not loaded")
        if company_name:
            return self.company_insights[
                self.company_insights['company_name'].str.contains(
                    company_name, case=False, na=False
                )
            ]
        return self.company_insights
