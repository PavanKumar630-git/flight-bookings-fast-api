from fastapi import FastAPI, Query
import requests

app = FastAPI(title="Flight Booking API", description="APIs for retrieving booking details", version="1.0")


def get_akasa_token():
    headers = {
        'accept': 'application/json, text/plain, */*',
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

    headers = {
        'accept': 'application/json',
        'authorization': akasaair_token,
        'origin': 'https://www.akasaair.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }
    params = {'recordLocator': pnr, 'lastName': lastname}

    try:
        response = requests.get('https://prod-bl.qp.akasaair.com/api/ibe/booking/retrieve', params=params, headers=headers)
        return response.json()
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
        return response.json()
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
        'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
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



@app.get("/digital_kuwaitairways")
def get_digital_kuwaitairwaysdetails(pnr: str = Query(...), lastname: str = Query(...)):
    headers = {
        'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
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


@app.get("/shop_lufthansa")
def get_shop_lufthansa_details(pnr: str = Query(...), lastname: str = Query(...)):
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



@app.get("/malaysiaairlines")
def get_malaysiaairlines_booking_details(pnr: str = Query(...), lastname: str = Query(...)):
    headers = {
        'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
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


