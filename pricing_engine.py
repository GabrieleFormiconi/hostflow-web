def calculate_suggested_price(competitors, base_price):
    if base_price is None:
        base_price = 100

    suggested_price = float(base_price)

    available_competitors = [
        c for c in competitors
        if c.get("available") is True
    ]

    unavailable_competitors = [
        c for c in competitors
        if c.get("available") is False
    ]

    available_count = len(available_competitors)
    unavailable_count = len(unavailable_competitors)
    total_competitors = len(competitors)

    if total_competitors > 0:
        available_ratio = available_count / total_competitors
    else:
        available_ratio = 0

    prices = [
        c.get("price")
        for c in competitors
        if c.get("price") is not None
    ]

    if prices:
        median_price = sum(prices) / len(prices)
    else:
        median_price = suggested_price

    # Regole pricing
    if available_ratio < 0.3:
        suggested_price *= 1.10
    elif available_ratio > 0.7:
        suggested_price *= 0.90

    # Se abbiamo prezzi reali competitor, usiamo anche quelli
    if prices:
        suggested_price = (suggested_price + median_price) / 2

    return {
        "base_price": round(base_price, 2),
        "median_price": round(median_price, 2),
        "available_ratio": round(available_ratio, 2),
        "suggested_price": round(suggested_price, 2),
        "available_count": available_count,
        "unavailable_count": unavailable_count,
        "total_competitors": total_competitors
    }
