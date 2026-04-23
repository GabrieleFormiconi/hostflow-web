import math
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)

GEOAPIFY_API_KEY = os.getenv("GEOAPIFY_API_KEY")


PROPERTY_TYPE_MAPPING = {
    "Appartamento intero": ["apartment", "holiday_home", "accommodation"],
    "Casa vacanze": ["holiday_home", "accommodation"],
    "Stanza privata": ["guest_house", "hostel", "accommodation"],
    "Hotel": ["hotel", "accommodation"],
    "Altro": ["accommodation"],
}


def haversine_distance(lat1, lon1, lat2, lon2):
    r = 6371

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _normalize_name(name):
    if not name:
        return "Struttura senza nome"
    return str(name).strip()


def _is_same_property(source_lat, source_lon, target_lat, target_lon, threshold_km=0.08):
    distance = haversine_distance(source_lat, source_lon, target_lat, target_lon)
    return distance <= threshold_km


def _estimate_guest_capacity(name, categories):
    name_lower = str(name).lower()
    categories_text = " ".join(categories).lower() if categories else ""

    if any(word in name_lower for word in ["suite", "family", "villa", "house", "home"]):
        return 4

    if any(word in name_lower for word in ["hotel", "inn", "resort"]):
        return 2

    if "apartment" in categories_text or "holiday_home" in categories_text:
        return 4

    if "guest_house" in categories_text:
        return 3

    if "hostel" in categories_text:
        return 2

    return 2


def _estimate_price(distance_km, rating, guests):
    base = 85

    if distance_km <= 1:
        base += 20
    elif distance_km <= 2:
        base += 10
    elif distance_km <= 5:
        base += 5

    if rating >= 9:
        base += 25
    elif rating >= 8.5:
        base += 18
    elif rating >= 8:
        base += 12
    elif rating >= 7:
        base += 8

    if guests >= 4:
        base += 20
    elif guests == 3:
        base += 10

    return round(base)


def _estimate_rating(name):
    name_lower = str(name).lower()

    if any(word in name_lower for word in ["hilton", "marriott", "qc", "resort"]):
        return 8.9

    if any(word in name_lower for word in ["suite", "apartment", "house", "home"]):
        return 8.4

    return 7.8


def _estimate_reviews_count(rating):
    if rating >= 9:
        return 220
    if rating >= 8.5:
        return 120
    if rating >= 8:
        return 75
    return 35


def _estimate_availability(checkin, checkout, distance_km):
    if distance_km <= 1.5:
        return True
    if distance_km <= 3:
        return True
    if distance_km <= 5:
        return True
    return False


def _fetch_geoapify_places(lat, lon, radius_km, limit):
    radius_m = int(radius_km * 1000)
    url = "https://api.geoapify.com/v2/places"

    params = {
        "categories": "accommodation",
        "filter": f"circle:{lon},{lat},{radius_m}",
        "bias": f"proximity:{lon},{lat}",
        "limit": limit,
        "apiKey": GEOAPIFY_API_KEY,
    }

    response = requests.get(url, params=params, timeout=20)

    print("=== COMPETITOR SERVICE AVVIATO ===")
    print("BASE_DIR:", BASE_DIR)
    print("ENV_PATH:", ENV_PATH)
    print("ENV FILE EXISTS:", ENV_PATH.exists())
    print("API KEY PRESENTE:", bool(GEOAPIFY_API_KEY))
    print("LAT:", lat)
    print("LON:", lon)
    print("RADIUS_KM:", radius_km)
    print("LIMIT:", limit)
    print("STATUS CODE:", response.status_code)
    print("URL:", response.url)
    print("RAW RESPONSE:", response.text[:1000])

    response.raise_for_status()
    return response.json()


def get_competitors(
    lat,
    lon,
    radius_km=2,
    checkin=None,
    checkout=None,
    guests=None,
    property_type=None,
    limit=100,
):
    if not GEOAPIFY_API_KEY:
        print("Errore: GEOAPIFY_API_KEY non trovata nel file .env")
        return []

    try:
        data = _fetch_geoapify_places(lat, lon, radius_km, limit)
        competitors = []
        seen = set()

        allowed_types = PROPERTY_TYPE_MAPPING.get(property_type, ["accommodation"])

        maximum_required_guests = None
        if guests:
            maximum_required_guests = int(guests)

        for feature in data.get("features", []):
            props = feature.get("properties", {})

            comp_lat = props.get("lat")
            comp_lon = props.get("lon")

            if comp_lat is None or comp_lon is None:
                continue

            distance = haversine_distance(lat, lon, comp_lat, comp_lon)
            same_property = _is_same_property(lat, lon, comp_lat, comp_lon)

            name = _normalize_name(props.get("name"))
            address = props.get("formatted", "") or ""
            categories = props.get("categories", [])

            categories_text = " ".join(categories).lower()

            if property_type:
                if not any(t.lower() in categories_text for t in allowed_types):
                    continue

            estimated_guests = _estimate_guest_capacity(name, categories)

            if maximum_required_guests and estimated_guests > maximum_required_guests:
                continue

            dedupe_key = (
                name.lower(),
                address.lower(),
                round(comp_lat, 5),
                round(comp_lon, 5),
            )

            if dedupe_key in seen:
                continue

            seen.add(dedupe_key)

            rating = _estimate_rating(name)
            reviews_count = _estimate_reviews_count(rating)
            available = _estimate_availability(checkin, checkout, distance)
            price = _estimate_price(distance, rating, estimated_guests)

            competitor = {
                "platform": "Geoapify",
                "name": name,
                "address": address,
                "lat": comp_lat,
                "lon": comp_lon,
                "distance_km": round(distance, 2),
                "category": categories,
                "category_label": categories[0] if categories else "Accommodation",
                "guests": estimated_guests,
                "bedrooms": 1 if estimated_guests <= 2 else 2,
                "bathrooms": 1,
                "rating": rating,
                "reviews_count": reviews_count,
                "price": price,
                "available": available,
                "checkin": checkin,
                "checkout": checkout,
                "property_type": property_type,
                "is_same_property": same_property,
            }

            competitors.append(competitor)

        competitors.sort(key=lambda x: x["distance_km"])

        print("COMPETITOR TROVATI:", len(competitors))
        return competitors

    except Exception as e:
        print("Errore chiamata Geoapify:", e)
        return []


if __name__ == "__main__":
    lat = 41.79908
    lon = 12.30236

    competitors = get_competitors(
        lat=lat,
        lon=lon,
        radius_km=5,
        checkin="2026-04-10",
        checkout="2026-04-12",
        guests=4,
        property_type="Appartamento intero",
        limit=100,
    )

    print("\nCompetitor trovati:\n")
    for c in competitors:
        print(c)
