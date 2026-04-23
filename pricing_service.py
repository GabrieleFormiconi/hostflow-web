from geocoding_service import geocode_address
from competitor_service import get_competitors
from pricing_engine import calculate_suggested_price


def _safe_get_competitors(lat, lon, radius_km, checkin=None, checkout=None, guests=None, property_type=None):
    """
    Prova prima a chiamare get_competitors con i nuovi parametri.
    Se competitor_service.py non è ancora aggiornato e accetta solo 3 parametri,
    fa fallback automatico alla vecchia firma senza rompere l'app.
    """
    try:
        return get_competitors(
            lat=lat,
            lon=lon,
            radius_km=radius_km,
            checkin=checkin,
            checkout=checkout,
            guests=guests,
            property_type=property_type,
        )
    except TypeError:
        return get_competitors(lat, lon, radius_km)


def _normalize_pricing_result(pricing_result, competitors, base_price):
    pricing_result = pricing_result or {}

    prices = []
    available_count = 0
    unavailable_count = 0

    for c in competitors:
        price = c.get("price")
        available = c.get("available")

        try:
            if price is not None:
                prices.append(float(price))
        except Exception:
            pass

        if available is True:
            available_count += 1
        elif available is False:
            unavailable_count += 1

    total_competitors = len(competitors)
    average_price = round(sum(prices) / len(prices), 2) if prices else 0.0
    median_price = pricing_result.get("median_price", 0.0) or 0.0
    min_price = min(prices) if prices else 0.0
    max_price = max(prices) if prices else 0.0
    available_ratio = round(available_count / total_competitors, 2) if total_competitors else 0.0

    normalized = {
        "base_price": float(pricing_result.get("base_price", base_price) or base_price),
        "median_price": float(median_price),
        "average_price": float(pricing_result.get("average_price", average_price) or average_price),
        "min_price": float(pricing_result.get("min_price", min_price) or min_price),
        "max_price": float(pricing_result.get("max_price", max_price) or max_price),
        "available_ratio": float(pricing_result.get("available_ratio", available_ratio) or available_ratio),
        "suggested_price": float(pricing_result.get("suggested_price", base_price) or base_price),
        "available_count": int(pricing_result.get("available_count", available_count) or available_count),
        "unavailable_count": int(pricing_result.get("unavailable_count", unavailable_count) or unavailable_count),
        "total_competitors": int(pricing_result.get("total_competitors", total_competitors) or total_competitors),
    }

    return normalized


def run_pricing_analysis(
    address,
    radius_km,
    base_price,
    checkin=None,
    checkout=None,
    guests=None,
    property_type=None,
):
    geo_result = geocode_address(address)

    if not geo_result:
        return {"error": "Indirizzo non trovato"}

    lat = geo_result["lat"]
    lon = geo_result["lon"]

    competitors = _safe_get_competitors(
        lat=lat,
        lon=lon,
        radius_km=radius_km,
        checkin=checkin,
        checkout=checkout,
        guests=guests,
        property_type=property_type,
    )

    pricing_result = calculate_suggested_price(competitors, base_price)
    pricing_result = _normalize_pricing_result(pricing_result, competitors, base_price)

    return {
        "address": geo_result.get("formatted", address),
        "lat": lat,
        "lon": lon,
        "competitors": competitors,
        "pricing": pricing_result
    }


if __name__ == "__main__":
    result = run_pricing_analysis(
        address="Via del Corso 1, Roma, Italia",
        radius_km=2,
        base_price=130,
        checkin="2026-04-10",
        checkout="2026-04-12",
        guests=2,
        property_type="Appartamento intero",
    )

    print(result)
