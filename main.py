from fastapi import FastAPI, Query
import requests

import datetime

# from flask import Flask, request, jsonify, send_file
# from fake_useragent import UserAgent
# import dateutil.parser
# import sqlalchemy
# import datetime
# import hashlib
# import uuid
import pyodbc
# from itertools import islice
# import random
# from azcaptchaapi import AZCaptchaApi
# import os
# import requests
# import re
# from selenium.webdriver.chrome.options import Options
# from selenium import webdriver
# import pickle
# import json
# from twocaptcha import TwoCaptcha
# from PIL import Image
# from bs4 import BeautifulSoup
# from selenium.webdriver.support.ui import Select
# import time
# import pandas as pd
# from selenium.webdriver.common.by import By
# from io import BytesIO
# from selenium.webdriver.common.keys import Keys
import warnings
warnings.filterwarnings("ignore")
#import undetected_chromedriver as uc
# import threading
# from concurrent.futures import ThreadPoolExecutor
import sys
del sys.modules['datetime']
# from datetime import datetime, timedelta


app = FastAPI(title="Flight Booking API", description="APIs for retrieving booking details", version="1.0")

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=61.246.34.128,9042;"  # Use comma to specify port
    "DATABASE=PTS_B2B_TEST;"
    "UID=sa;"
    "PWD=Server$#@54321;"
)

connection = pyodbc.connect(conn_str)
print("DB Connected Successfully")
cursor = connection.cursor()


insert_query_fligh_info = """
    INSERT INTO FLT_Info_DEMO (
        FLT_HeaderFK, SectorRef, AirlineCode, FlightNo, DepCityCode, ArrCityCode,
        DepTerminal, ArrTerminal, DepDate, ArrDate, DepTime, ArrTime, BookingDate,
        ServiceClass, Cabin, Duration, AirlinePNR, AdtCabin, ChdCabin, InfCabin,
        AdtFareBasis, ChdFareBasis, InfFareBasis, AdtRbd, ChdRbd, InfRbd,
        AdtFareType, ChdFareType, InfFareType, triptypecode, CreationDate, UpdationDate
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """
akasaair_insert_query = """INSERT INTO FLT_HEADER_DEMO (
    JourneyTypeCode,
    RequestTypeCode,
    TripTypeCode,
    GDSPNR,
    TotalBookingAmount,
    NumAdult,
    NumChild,
    NumInfant,
    ValidatingCarrier,
    BookingStatus,
    PNRStatus,
    CorporateCode,
    CreationDate,
    UpdationDate,
    IsDeleted,
    ExecutiveId,
    UserID,
    MordifyStatus,
    Sector,
    BookingRefNo,
    Remark,
    CommData,
    NetFare,
    Email,
    MobileNo,
    Address,
    City,
    State,
    ZipCode,
    CountryCode,
    CountryName,
    PlanType,
    VendorCode,
    AccountCode,
    Agent_Amount,
    Agent_Comm_Amount,
    myTotalFare,
    myNetFAre,
    mytds,
    myst,
    ot_charges,
    ot_total_charges,
    status,
    lock_by,
    emulatedby,
    billing_status,
    bill_no,
    tk_vendor,
    tk_cus_cod,
    requestno,
    bhel_staff_code,
    bhel_staff_travel_code,
    emailid_api,
    bhel_staff_check,
    old_ticketno,
    loginid,
    vouchrefno,
    party_verified,
    cc_set,
    Bill_Date,
    ipAddress,
    gst_number,
    gst_name,
    gst_email,
    gst_address,
    gst_mobile,
    gst_city,
    gst_state,
    gst_pincode,
    edited_by,
    issued_by,
    taxinvoiceno,
    creditinvoiceno,
    taxinvoicedate,
    creditinvoicedate,
    Party_Verified_Time,
    PurchaseVendor,
    VenCommGiven,
    SegmentType,
    issuedbycc,
    creditcardno,
    responsebillno,
    OTPCode,
    dealmodifyby,
    dealmodifydate,
    dealmodify,
    Remarks,
    LtdVendorCode,
    LtdBlivNo,
    Vendorinvnumber,
    GTID,
    VendorGSTNumber
)
VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?
)"""


insert_query_air_express = """
INSERT INTO FLT_HEADER_DEMO (
    FLT_HeaderID, JourneyTypeCode, RequestTypeCode, TripTypeCode, GDSPNR,
    TotalBookingAmount, NumAdult, NumChild, NumInfant, ValidatingCarrier,
    BookingStatus, PNRStatus, CorporateCode, CreationDate, UpdationDate,
    IsDeleted, ExecutiveId, UserID, MordifyStatus, Sector, BookingRefNo,
    Remark, CommData, NetFare, Email, MobileNo, Address, City, State,
    ZipCode, CountryCode, CountryName, PlanType, VendorCode, AccountCode,
    Agent_Amount, Agent_Comm_Amount, myTotalFare, myNetFAre, mytds, myst,
    ot_charges, ot_total_charges, status, lock_by, emulatedby, billing_status,
    bill_no, tk_vendor, tk_cus_cod, requestno, bhel_staff_code, bhel_staff_travel_code,
    emailid_api, bhel_staff_check, old_ticketno, loginid, vouchrefno, party_verified,
    cc_set, Bill_Date, ipAddress, gst_number, gst_name, gst_email, gst_address,
    gst_mobile, gst_city, gst_state, gst_pincode, edited_by, issued_by,
    taxinvoiceno, creditinvoiceno, taxinvoicedate, creditinvoicedate,
    Party_Verified_Time, PurchaseVendor, VenCommGiven, SegmentType, issuedbycc,
    creditcardno, responsebillno, OTPCode, dealmodifyby, dealmodifydate,
    dealmodify, Remarks, LtdVendorCode, LtdBlivNo, Vendorinvnumber, GTID, VendorGSTNumber
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?
);"""
def akasaair_flight_info_data(json_data):
    """
    Extracts flight information data from JSON and maps to FLT_Info table structure
    """
    try:
        data = json_data.get('data', {})
        journeys = data.get('journeys', [])

        flight_info_records = []

        for journey in journeys:
            for segment in journey.get('segments', []):
                designator = segment.get('designator', {})
                leg_info = segment.get('legs', [{}])[0].get('legInfo', {})

                # Extract basic flight info
                flight_info = {
                    'FLT_HeaderFK': None,  # Will be set after FLT_Header insert
                    'SectorRef': f"{designator.get('origin', '')}-{designator.get('destination', '')}",
                    'AirlineCode': segment.get('identifier', {}).get('carrierCode'),
                    'FlightNo': segment.get('identifier', {}).get('identifier'),
                    'DepCityCode': designator.get('origin'),
                    'ArrCityCode': designator.get('destination'),
                    'DepAirportName': None,  # Not in JSON
                    'ArrAirportName': None,  # Not in JSON
                    'DepTerminal': leg_info.get('departureTerminal'),
                    'ArrTerminal': leg_info.get('arrivalTerminal'),
                    'DepDate': designator.get('departure'),
                    'ArrDate': designator.get('arrival'),
                    'DepTime': designator.get('departure'),
                    'ArrTime': designator.get('arrival'),
                    'BookingDate': segment.get('salesDate'),
                    'ServiceClass': segment.get('fares', [{}])[0].get('travelClassCode'),
                    'Cabin': segment.get('fares', [{}])[0].get('classOfService'),
                    'Equipment': None,  # Not in JSON
                    'AvailabilityStatus': None,  # Not in JSON
                    'Duration': None,  # Can be calculated from dep/arr times
                    'AirlinePNR': data.get('recordLocator'),
                    'AdtCabin': None,  # Will be populated from passenger segments
                    'ChdCabin': None,
                    'InfCabin': None,
                    'AdtFareBasis': None,
                    'ChdFareBasis': None,
                    'InfFareBasis': None,
                    'AdtRbd': None,
                    'ChdRbd': None,
                    'InfRbd': None,
                    'AdtFareType': None,
                    'ChdFareType': None,
                    'InfFareType': None,
                    'triptypecode': journey.get('flightType')
                }

                # Process passenger segments for cabin and fare info
                for pax_segment in segment.get('passengerSegment', []):
                    for seat in pax_segment.get('seats', []):
                        compartment = seat.get('compartmentDesignator')
                        pax_type = next(
                            (p.get('passengerTypeCode')
                             for p in data.get('passengers', [])
                             if p.get('passengerKey') == pax_segment.get('passengerKey')),
                            'ADT'
                        )

                        if pax_type == 'ADT':
                            flight_info['AdtCabin'] = compartment
                        elif pax_type == 'CHD':
                            flight_info['ChdCabin'] = compartment
                        elif pax_type == 'INF':
                            flight_info['InfCabin'] = compartment

                # Process fares for fare basis and type
                for fare in segment.get('fares', []):
                    for pax_fare in fare.get('passengerFares', []):
                        pax_type = pax_fare.get('passengerType', 'ADT')
                        fare_basis = fare.get('fareBasisCode')

                        if pax_type == 'ADT':
                            flight_info['AdtFareBasis'] = fare_basis
                            flight_info['AdtRbd'] = fare.get('classOfService')
                        elif pax_type == 'CHD':
                            flight_info['ChdFareBasis'] = fare_basis
                            flight_info['ChdRbd'] = fare.get('classOfService')
                        elif pax_type == 'INF':
                            flight_info['InfFareBasis'] = fare_basis
                            flight_info['InfRbd'] = fare.get('classOfService')

                # Calculate duration if we have both departure and arrival
                if flight_info['DepTime'] and flight_info['ArrTime']:
                    dep_time = datetime.fromisoformat(flight_info['DepTime'])
                    arr_time = datetime.fromisoformat(flight_info['ArrTime'])
                    flight_info['Duration'] = str(arr_time - dep_time)

                flight_info_records.append(flight_info)

        return flight_info_records

    except Exception as e:
        print(f"Error extracting flight info data: {str(e)}")
        return []


def akasaair_insert_flight_data(json_data):
    try:
        data = json_data.get('data', {})
        breakdown = data.get('breakdown', {})
        journeys = data.get('journeys', [])
        contacts = data.get('contacts', [{}])[0]
        passengers = data.get('passengers', [{}])[0]
        address = contacts.get('address', {})
        name = contacts.get('name', {})
        passenger_fees = passengers.get('fees', [{}])[0] if passengers else {}
        type_of_sale = data.get('typeOfSale', {})
        info = data.get('info', {})

        # Calculate total taxes and fees
        journey_totals = breakdown.get('journeyTotals', {})
        total_tax = journey_totals.get('totalTax', 0)
        total_discount = journey_totals.get('totalDiscount', 0)

        # Calculate sector and journey type
        sectors = []
        origins = set()
        destinations = set()
        for journey in journeys:
            designator = journey.get('designator', {})
            origin = designator.get('origin', '')
            destination = designator.get('destination', '')
            if origin and destination:
                sectors.append(f"{origin}-{destination}")
                origins.add(origin)
                destinations.add(destination)

        is_return_trip = len(origins) > 1 or origins == destinations
        journey_type = "Return" if is_return_trip else "OneWay"

        # Passenger counts
        passenger_counts = {'ADT': 0, 'CHD': 0, 'INF': 0}
        for p in data.get('passengers', []):
            p_type = p.get('passengerTypeCode', 'ADT')
            passenger_counts[p_type] += 1

        # Get fare details
        base_fare = journey_totals.get('totalAmount', 0)
        total_fare = breakdown.get('totalAmount', 0)
        net_fare = base_fare - total_discount

        # Phone numbers
        phone_numbers = contacts.get('phoneNumbers', [{}])
        mobile_no = phone_numbers[0].get('number') if phone_numbers else None

        # Create the flight data dictionary with all possible fields
        flight_data = {
            # Basic Information
            'FLT_HeaderPK': None,
            'FLT_HeaderID': data.get('bookingKey'),
            'JourneyTypeCode': journey_type,
            'RequestTypeCode': 'Corporate' if type_of_sale.get('isCorporateFare') else 'Normal',
            'TripTypeCode': 'Domestic',
            'GDSPNR': data.get('recordLocator'),
            'TotalBookingAmount': total_fare,
            'NumAdult': passenger_counts['ADT'],
            'NumChild': passenger_counts['CHD'],
            'NumInfant': passenger_counts['INF'],
            'ValidatingCarrier': data.get('owningCarrierCode'),
            'BookingStatus': info.get('status'),
            'PNRStatus': info.get('paidStatus'),
            'CorporateCode': type_of_sale.get('promotionCode'),

            # Dates
            'CreationDate': info.get('createdDate'),
            'UpdationDate': info.get('modifiedDate'),
            'IsDeleted': False,

            # User/Agent Information
            'ExecutiveId': None,
            'UserID': contacts.get('sourceOrganization'),
            'MordifyStatus': 'Modified' if info.get('changeAllowed') else 'Original',

            # Flight Details
            'Sector': ', '.join(sectors),
            'BookingRefNo': data.get('recordLocator'),
            'Remark': passenger_fees.get('note'),
            'CommData': None,
            'NetFare': net_fare,

            # Contact Information
            'Email': contacts.get('emailAddress'),
            'MobileNo': mobile_no,
            'Address': f"{address.get('lineOne', '')} {address.get('lineTwo', '')}".strip(),
            'City': address.get('city'),
            'State': address.get('provinceState'),
            'ZipCode': address.get('postalCode'),
            'CountryCode': address.get('countryCode'),
            'CountryName': 'India',

            # Financial Information
            'PlanType': None,
            'VendorCode': None,
            'AccountCode': None,
            'Agent_Amount': None,
            'Agent_Comm_Amount': None,
            'myTotalFare': total_fare,
            'myNetFAre': net_fare,
            'mytds': None,
            'myst': None,
            'ot_charges': None,
            'ot_total_charges': None,

            # Status Information
            'status': info.get('status'),
            'lock_by': None,
            'emulatedby': None,
            'billing_status': 'Paid' if info.get('paidStatus') == 'PaidInFull' else 'Unpaid',
            'bill_no': None,
            'tk_vendor': None,
            'tk_cus_cod': None,
            'requestno': None,
            'bhel_staff_code': None,
            'bhel_staff_travel_code': None,
            'emailid_api': contacts.get('emailAddress'),
            'bhel_staff_check': None,
            'old_ticketno': None,
            'loginid': None,
            'vouchrefno': None,
            'party_verified': None,
            'cc_set': None,
            'Bill_Date': info.get('createdDate'),

            # GST Information (not present in sample data)
            'ipAddress': None,
            'gst_number': None,
            'gst_name': None,
            'gst_email': None,
            'gst_address': None,
            'gst_mobile': None,
            'gst_city': None,
            'gst_state': None,
            'gst_pincode': None,

            # Additional Fields
            'edited_by': None,
            'issued_by': None,
            'taxinvoiceno': None,
            'creditinvoiceno': None,
            'taxinvoicedate': None,
            'creditinvoicedate': None,
            'Party_Verified_Time': None,
            'PurchaseVendor': None,
            'VenCommGiven': None,
            'SegmentType': 'Domestic',
            'issuedbycc': None,
            'creditcardno': None,
            'responsebillno': None,
            'OTPCode': None,
            'dealmodifyby': None,
            'dealmodifydate': None,
            'dealmodify': None,
            'Remarks': passenger_fees.get('note'),
            'LtdVendorCode': None,
            'LtdBlivNo': None,
            'Vendorinvnumber': None,
            'GTID': None,
            'VendorGSTNumber': None
        }

        return flight_data

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return None



def extract_flight_air_express_header_demo_data(json_data):
    """
    Extracts flight header data from JSON for FLT_HEADER_DEMO table
    Args:
        json_data: The JSON booking data
    Returns:
        Dictionary of data ready for insertion into FLT_HEADER_DEMO
    """
    try:
        data = json_data.get('data', {})
        breakdown = data.get('breakdown', {})
        journeys = data.get('journeys', [])
        contacts = data.get('contacts', {}).get('P', {})  # Using P (primary) contact
        passenger = data.get('passengers', [{}])[0]

        # Calculate sector
        sectors = []
        for journey in journeys:
            designator = journey.get('designator', {})
            sectors.append(f"{designator.get('origin')}-{designator.get('destination')}")

        # Prepare the flight header data with all 93 fields in correct order
        flight_data = {
            # Basic Information (1-15)
            'FLT_HeaderID': data.get('bookingKey'),
            'JourneyTypeCode': 'OneWay' if len(sectors) == 1 else 'Return',
            'RequestTypeCode': 'Normal',
            'TripTypeCode': 'Domestic',
            'GDSPNR': data.get('recordLocator'),
            'TotalBookingAmount': breakdown.get('totalAmount'),
            'NumAdult': sum(1 for p in data.get('passengers', []) if p.get('passengerTypeCode') == 'ADT'),
            'NumChild': sum(1 for p in data.get('passengers', []) if p.get('passengerTypeCode') == 'CHD'),
            'NumInfant': sum(1 for p in data.get('passengers', []) if p.get('passengerTypeCode') == 'INF'),
            'ValidatingCarrier': data.get('info', {}).get('owningCarrierCode'),
            'BookingStatus': 'Confirmed' if data.get('info', {}).get('status') == 2 else 'Pending',
            'PNRStatus': 'Paid' if data.get('info', {}).get('paidStatus') == 1 else 'Unpaid',
            'CorporateCode': data.get('typeOfSale', {}).get('promotionCode'),
            'CreationDate': data.get('info', {}).get('createdDate'),
            'UpdationDate': data.get('info', {}).get('modifiedDate'),

            # Status Fields (16-30)
            'IsDeleted': False,
            'ExecutiveId': None,
            'UserID': contacts.get('sourceOrganization'),
            'MordifyStatus': 'Modified' if data.get('info', {}).get('changeAllowed') else 'Original',
            'Sector': ', '.join(sectors),
            'BookingRefNo': data.get('recordLocator'),
            'Remark': passenger.get('fees', [{}])[0].get('note'),
            'CommData': None,
            'NetFare': breakdown.get('journeyTotals', {}).get('totalAmount'),
            'Email': contacts.get('emailAddress'),
            'MobileNo': contacts.get('phoneNumbers', [{}])[0].get('number'),
            'Address': contacts.get('address', {}).get('lineOne'),
            'City': contacts.get('address', {}).get('city'),
            'State': contacts.get('address', {}).get('provinceState'),

            # Location Fields (31-45)
            'ZipCode': contacts.get('address', {}).get('postalCode'),
            'CountryCode': contacts.get('address', {}).get('countryCode'),
            'CountryName': None,
            'PlanType': None,
            'VendorCode': None,
            'AccountCode': None,
            'Agent_Amount': None,
            'Agent_Comm_Amount': None,
            'myTotalFare': breakdown.get('totalAmount'),
            'myNetFAre': breakdown.get('journeyTotals', {}).get('totalAmount'),
            'mytds': None,
            'myst': None,
            'ot_charges': None,
            'ot_total_charges': None,
            'status': 'Active',

            # System Fields (46-60)
            'lock_by': None,
            'emulatedby': None,
            'billing_status': 'Paid' if data.get('info', {}).get('paidStatus') == 1 else 'Unpaid',
            'bill_no': None,
            'tk_vendor': None,
            'tk_cus_cod': None,
            'requestno': None,
            'bhel_staff_code': None,
            'bhel_staff_travel_code': None,
            'emailid_api': contacts.get('emailAddress'),
            'bhel_staff_check': None,
            'old_ticketno': None,
            'loginid': None,
            'vouchrefno': None,
            'party_verified': None,

            # Payment Fields (61-75)
            'cc_set': None,
            'Bill_Date': data.get('info', {}).get('createdDate'),
            'ipAddress': None,
            'gst_number': contacts.get('customerNumber'),
            'gst_name': contacts.get('companyName'),
            'gst_email': contacts.get('emailAddress'),
            'gst_address': contacts.get('address', {}).get('lineOne'),
            'gst_mobile': contacts.get('phoneNumbers', [{}])[0].get('number'),
            'gst_city': contacts.get('address', {}).get('city'),
            'gst_state': contacts.get('address', {}).get('provinceState'),
            'gst_pincode': contacts.get('address', {}).get('postalCode'),
            'edited_by': None,
            'issued_by': None,
            'taxinvoiceno': None,
            'creditinvoiceno': None,

            # Additional Fields (76-93)
            'taxinvoicedate': None,
            'creditinvoicedate': None,
            'Party_Verified_Time': None,
            'PurchaseVendor': None,
            'VenCommGiven': None,
            'SegmentType': 'Domestic',
            'issuedbycc': None,
            'creditcardno': None,
            'responsebillno': None,
            'OTPCode': None,
            'dealmodifyby': None,
            'dealmodifydate': None,
            'dealmodify': None,
            'Remarks': passenger.get('fees', [{}])[0].get('note'),
            'LtdVendorCode': None,
            'LtdBlivNo': None,
            'Vendorinvnumber': None,
            'GTID': None,
            'VendorGSTNumber': contacts.get('customerNumber')
        }

        # Verification
        expected_field_count = 93
        if len(flight_data) != expected_field_count:
            missing = expected_field_count - len(flight_data)
            raise ValueError(f"Flight data has {len(flight_data)} fields, expected {expected_field_count}. Missing {missing} fields.")

        return flight_data

    except Exception as e:
        print(f"Error extracting flight header demo data: {str(e)}")
        return None


conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=61.246.34.128,9042;"  # Replace with your actual server and port
    "DATABASE=PTS_B2B_TEST;"
    "UID=sa;"
    "PWD=Server$#@54321;"
)
# Function to create a pool of database connections
def create_connection_pool(pool_size):
    return [pyodbc.connect(conn_str) for _ in range(pool_size)]

# Create a pool of 5 connections
connection_pool = create_connection_pool(pool_size=5)

# Function to execute queries from the pool
# def execute_query(query, params=None):
#     connection = connection_pool.pop(0)  # Get a connection from the pool
#     try:
#         with connection.cursor() as cursor:
#             if params:
#                 cursor.execute(query, params)
#             else:
#                 cursor.execute(query)

#             if query.strip().upper().startswith("SELECT"):
#                 result = cursor.fetchall()
#                 return result
#             else:
#                 connection.commit()
#                 return "Query executed successfully"
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         print(query)
#         sys.exit()
#         return None
#     finally:
#         connection_pool.append(connection)

def execute_query(query, params=None, check_exists=None):
    """
    Executes a query with optional duplicate checking

    Args:
        query: SQL query to execute
        params: Parameters for the query (tuple or dict)
        check_exists: Dictionary with:
            'table': Table name to check
            'column': Column name to check against
            'value': Value to check for existence

    Returns:
        Query results if SELECT, status message otherwise
        "Duplicate found - not inserted" if duplicate exists
    """
    connection = connection_pool.pop(0)
    try:
        with connection.cursor() as cursor:
            # Check for existing record if requested
            if check_exists:
                table = check_exists['table']
                column = check_exists['column']
                value = check_exists['value']

                check_query = f"SELECT 1 FROM {table} WHERE {column} = ?"
                cursor.execute(check_query, (value,))
                if cursor.fetchone():
                    return "Duplicate PNR found - can not inserted the data"

            # Execute the main query
            if params:
                if isinstance(params, dict):
                    cursor.execute(query, **params)  # For named parameters
                else:
                    cursor.execute(query, params)  # For positional parameters
            else:
                cursor.execute(query)

            # Handle results based on query type
            if query.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
                return result
            else:
                connection.commit()
                return "Data Inserted"

    except Exception as e:
        print(f"Error executing query: {str(e)}")
        print(f"Query: {query}")
        if 'connection' in locals():
            connection.rollback()
        raise  # Re-raise the exception after handling
    finally:
        if 'connection' in locals():
            connection_pool.append(connection)

def get_akasa_token():
    headers = {
        'accept': 'application/json, text/plain, /',
        'content-type': 'application/json',
        'origin': 'https://www.akasaair.com',
        'referer': 'https://www.akasaair.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }
    json_data = {'clientType': 'WEB'}

    try:
        response = requests.post('https://prod-bl.qp.akasaair.com/api/ibe/token', headers=headers, json=json_data)
        response_data = response.json()
        return response_data.get('data', {}).get('token', None)
    except Exception as e:
        return {"error": str(e)}


def get_airindia_oauth_token():
    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://travel.airindia.com',
    }
    data = {
        'client_id': 'DCkj8EM4xxOUnINtcYcUhGXVfP2KKUzf',
        'client_secret': 'QWgBtA2ARMfdAf1g',
        'grant_type': 'client_credentials',
        'guest_office_id': 'DELAI08AA',
    }

    response = requests.post('https://api-des.airindia.com/v1/security/oauth2/token', headers=headers, data=data)
    return response.json()



@app.get("/akasaair_bookingdetails")
def get_akasaair_booking_details(pnr: str = Query(...), lastname: str = Query(...)):
    akasaair_token = get_akasa_token()
    # data = execute_query(select_query)
    # print("data:",data)
    headers = {
        'accept': 'application/json',
        'authorization': akasaair_token,
        'origin': 'https://www.akasaair.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }
    params = {'recordLocator': pnr, 'lastName': lastname}

    try:
        response = requests.get('https://prod-bl.qp.akasaair.com/api/ibe/booking/retrieve', params=params, headers=headers)
        try:
            maindata = response.json()
            flight_data = akasaair_insert_flight_data(maindata)
            params = (
        # flight_data['FLT_HeaderPK'],
        # flight_data['FLT_HeaderID'],
        flight_data['JourneyTypeCode'],
        flight_data['RequestTypeCode'],
        flight_data['TripTypeCode'],
        flight_data['GDSPNR'],
        flight_data['TotalBookingAmount'],
        flight_data['NumAdult'],
        flight_data['NumChild'],
        flight_data['NumInfant'],
        flight_data['ValidatingCarrier'],
        flight_data['BookingStatus'],
        flight_data['PNRStatus'],
        flight_data['CorporateCode'],
        flight_data['CreationDate'],
        flight_data['UpdationDate'],
        flight_data['IsDeleted'],
        flight_data['ExecutiveId'],
        flight_data['UserID'],
        flight_data['MordifyStatus'],
        flight_data['Sector'],
        flight_data['BookingRefNo'],
        flight_data['Remark'],
        flight_data['CommData'],
        flight_data['NetFare'],
        flight_data['Email'],
        flight_data['MobileNo'],
        flight_data['Address'],
        flight_data['City'],
        flight_data['State'],
        flight_data['ZipCode'],
        flight_data['CountryCode'],
        flight_data['CountryName'],
        flight_data['PlanType'],
        flight_data['VendorCode'],
        flight_data['AccountCode'],
        flight_data['Agent_Amount'],
        flight_data['Agent_Comm_Amount'],
        flight_data['myTotalFare'],
        flight_data['myNetFAre'],
        flight_data['mytds'],
        flight_data['myst'],
        flight_data['ot_charges'],
        flight_data['ot_total_charges'],
        flight_data['status'],
        flight_data['lock_by'],
        flight_data['emulatedby'],
        flight_data['billing_status'],
        flight_data['bill_no'],
        flight_data['tk_vendor'],
        flight_data['tk_cus_cod'],
        flight_data['requestno'],
        flight_data['bhel_staff_code'],
        flight_data['bhel_staff_travel_code'],
        flight_data['emailid_api'],
        flight_data['bhel_staff_check'],
        flight_data['old_ticketno'],
        flight_data['loginid'],
        flight_data['vouchrefno'],
        flight_data['party_verified'],
        flight_data['cc_set'],
        flight_data['Bill_Date'],
        flight_data['ipAddress'],
        flight_data['gst_number'],
        flight_data['gst_name'],
        flight_data['gst_email'],
        flight_data['gst_address'],
        flight_data['gst_mobile'],
        flight_data['gst_city'],
        flight_data['gst_state'],
        flight_data['gst_pincode'],
        flight_data['edited_by'],
        flight_data['issued_by'],
        flight_data['taxinvoiceno'],
        flight_data['creditinvoiceno'],
        flight_data['taxinvoicedate'],
        flight_data['creditinvoicedate'],
        flight_data['Party_Verified_Time'],
        flight_data['PurchaseVendor'],
        flight_data['VenCommGiven'],
        flight_data['SegmentType'],
        flight_data['issuedbycc'],
        flight_data['creditcardno'],
        flight_data['responsebillno'],
        flight_data['OTPCode'],
        flight_data['dealmodifyby'],
        flight_data['dealmodifydate'],
        flight_data['dealmodify'],
        flight_data['Remarks'],
        flight_data['LtdVendorCode'],
        flight_data['LtdBlivNo'],
        flight_data['Vendorinvnumber'],
        flight_data['GTID'],
        flight_data['VendorGSTNumber']
    )

            # try:
            sqlresult = execute_query(akasaair_insert_query,
                          params,
                          check_exists={
        'table': 'FLT_HEADER_DEMO',
        'column': 'GDSPNR',
        'value': flight_data['GDSPNR']
    }
                          )
            # except Exception as eee:
            #     print("eee:",eee)

            return {"finaldata":maindata,"message":sqlresult}

        except Exception as ee:
            return {"error":ee}


    except Exception as e:
        return {"error": str(e)}


@app.get("/airindia_express_bookingdetails")
def get_airindia_express_booking_details(pnr: str = Query(...), lastname: str = Query(...)):
    headers = {
        'accept': 'application/json',
        'authorization': 'undefined',
        'content-type': 'application/json',
        'ocp-apim-subscription-key': 'fe65ec9eec2445d9802be1d6c0295158',
        'origin': 'https://www.airindiaexpress.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }
    json_data = {'addtnlDetail': lastname, 'recordLocator': pnr, 'sessionType': 'WebAnonUser'}

    try:
        response = requests.post(
            'https://api.airindiaexpress.com/b2c-CheckIn/v2/mmb/retrieve/byRecordLocator',
            headers=headers,
            json=json_data,
        )

        maindata = response.json()
        flight_data = extract_flight_air_express_header_demo_data(maindata)
        params = (
           flight_data['FLT_HeaderID'],
        flight_data['JourneyTypeCode'],
        flight_data['RequestTypeCode'],
        flight_data['TripTypeCode'],
        flight_data['GDSPNR'],
        flight_data['TotalBookingAmount'],
        flight_data['NumAdult'],
        flight_data['NumChild'],
        flight_data['NumInfant'],
        flight_data['ValidatingCarrier'],
        flight_data['BookingStatus'],
        flight_data['PNRStatus'],
        flight_data['CorporateCode'],
        flight_data['CreationDate'],
        flight_data['UpdationDate'],
        flight_data['IsDeleted'],
        flight_data['ExecutiveId'],
        flight_data['UserID'],
        flight_data['MordifyStatus'],
        flight_data['Sector'],
        flight_data['BookingRefNo'],
        flight_data['Remark'],
        flight_data['CommData'],
        flight_data['NetFare'],
        flight_data['Email'],
        flight_data['MobileNo'],
        flight_data['Address'],
        flight_data['City'],
        flight_data['State'],
        flight_data['ZipCode'],
        flight_data['CountryCode'],
        flight_data['CountryName'],
        flight_data['PlanType'],
        flight_data['VendorCode'],
        flight_data['AccountCode'],
        flight_data['Agent_Amount'],
        flight_data['Agent_Comm_Amount'],
        flight_data['myTotalFare'],
        flight_data['myNetFAre'],
        flight_data['mytds'],
        flight_data['myst'],
        flight_data['ot_charges'],
        flight_data['ot_total_charges'],
        flight_data['status'],
        flight_data['lock_by'],
        flight_data['emulatedby'],
        flight_data['billing_status'],
        flight_data['bill_no'],
        flight_data['tk_vendor'],
        flight_data['tk_cus_cod'],
        flight_data['requestno'],
        flight_data['bhel_staff_code'],
        flight_data['bhel_staff_travel_code'],
        flight_data['emailid_api'],
        flight_data['bhel_staff_check'],
        flight_data['old_ticketno'],
        flight_data['loginid'],
        flight_data['vouchrefno'],
        flight_data['party_verified'],
        flight_data['cc_set'],
        flight_data['Bill_Date'],
        flight_data['ipAddress'],
        flight_data['gst_number'],
        flight_data['gst_name'],
        flight_data['gst_email'],
        flight_data['gst_address'],
        flight_data['gst_mobile'],
        flight_data['gst_city'],
        flight_data['gst_state'],
        flight_data['gst_pincode'],
        flight_data['edited_by'],
        flight_data['issued_by'],
        flight_data['taxinvoiceno'],
        flight_data['creditinvoiceno'],
        flight_data['taxinvoicedate'],
        flight_data['creditinvoicedate'],
        flight_data['Party_Verified_Time'],
        flight_data['PurchaseVendor'],
        flight_data['VenCommGiven'],
        flight_data['SegmentType'],
        flight_data['issuedbycc'],
        flight_data['creditcardno'],
        flight_data['responsebillno'],
        flight_data['OTPCode'],
        flight_data['dealmodifyby'],
        flight_data['dealmodifydate'],
        flight_data['dealmodify'],
        flight_data['Remarks'],
        flight_data['LtdVendorCode'],
        flight_data['LtdBlivNo'],
        flight_data['Vendorinvnumber'],
        flight_data['GTID'],
        flight_data['VendorGSTNumber'],
        flight_data['OTPCode'],
        flight_data['dealmodifyby'],
        flight_data['dealmodifydate'],
        flight_data['dealmodify'],
        flight_data['Remarks'],
        flight_data['LtdVendorCode'],
        flight_data['LtdBlivNo'],
        flight_data['Vendorinvnumber'],
        flight_data['GTID'],
        flight_data['VendorGSTNumber']
       )

        try:

            table_insert = execute_query(insert_query_air_express,params, check_exists={
    'table': 'FLT_HEADER_DEMO',
    'column': 'GDSPNR',
    'value': flight_data['GDSPNR']}
                )
            return {"data":maindata,"message":table_insert}
        except Exception as ee:
            return {"error":ee}

    except Exception as e:
        return {"error": str(e)}


@app.get("/airindia_bookingdetails")
def get_airindia_booking_details(pnr: str = Query(...), lastname: str = Query(...)):
    airindia_token = get_airindia_oauth_token()

    headers = {
        'accept': 'application/json',
        'authorization': f"{airindia_token['token_type']} {airindia_token['access_token']}",
        'content-type': 'application/json',
        'origin': 'https://travel.airindia.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }
    params = {'lastName': lastname, 'showOrderEligibilities': 'true', 'checkServicesAndSeatsIssuanceCurrency': 'false'}

    try:
        response = requests.get(f'https://api-des.airindia.com/v2/purchase/orders/{pnr}', params=params, headers=headers)
        return response.json()
    except Exception as e:
        return {"error": str(e)}


@app.get("/spicejet_bookingdetails")
def get_spicejet_booking_details(pnr: str = Query(...), lastname: str = Query(...)):
    headers = {
        'Authorization': "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkb3RSRVogQVBJIiwianRpIjoiYjM1OTZjYWYtNzIxMC04ODIzLTFjYWUtNzBjOWZkZTU4NmRiIiwiaXNzIjoiZG90UkVaIEFQSSJ9.q8v8McYrEoRiDin3hZ0tZkjEdXd1kAuv5tbU1-KKsoA",
        'Referer': f'https://www.spicejet.com/trips/details?pnr={pnr}&last={lastname}',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Content-Type': 'application/json',
        'os': 'desktop',
    }
    params = {'recordLocator': pnr, 'lastName': lastname}
    json_data = {'userData': {}, 'method': 'GET'}

    try:
        response = requests.post('https://www.spicejet.com/api/v1/booking/retrieveBookingByPNR', params=params, headers=headers, json=json_data)
        return response.json()
    except Exception as e:
        return {"error": str(e)}
