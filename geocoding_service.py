import requests

API_KEY = "99ce7a21fd8c4485841215685a30183d"

def geocode_address(address):
    url = "https://api.geoapify.com/v1/geocode/search"

    params = {
        "text": address,
        "lang": "it",
        "limit": 10,
        "apiKey": API_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=20)

        if response.status_code != 200:
            print("ERRORE STATUS CODE:", response.status_code)
            print(response.text)
            return None

        data = response.json()
        features = data.get("features", [])

        if not features:
            print("Nessun risultato trovato da Geoapify")
            return None

        selected = features[0].get("properties", {})

        return {
            "lat": selected["lat"],
            "lon": selected["lon"],
            "formatted": selected.get("formatted", address)
        }

    except Exception as e:
        print("ERRORE GEOCODING:", e)
        return None
