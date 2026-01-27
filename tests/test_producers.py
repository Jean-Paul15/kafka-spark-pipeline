"""Tests des générateurs d'événements (aucun broker Kafka requis)."""
import ecommerce_producer as ecom
import immobilier_producer as immo
import nyc_taxi_producer as taxi


def test_ecommerce_transaction_has_expected_sections():
    tx = ecom.generate_transaction()
    for key in ("transaction_id", "timestamp", "customer", "product", "payment", "session"):
        assert key in tx
    assert tx["product"]["quantity"] >= 1


def test_taxi_trip_has_pickup_dropoff_and_fare():
    trip = taxi.simulate_trip()
    for key in ("trip_id", "pickup", "dropoff", "trip", "fare"):
        assert key in trip
    assert trip["trip"]["distance_miles"] > 0


def test_immobilier_annonce_price_is_consistent():
    a = immo.generate_annonce()
    assert a["surface_m2"] > 0
    assert a["prix_m2_xof"] == int(a["prix_xof"] / a["surface_m2"])


def test_immobilier_prix_trend_matches_variation():
    p = immo.generate_prix()
    assert p["tendance"] == ("hausse" if p["variation_pct"] > 0 else "baisse")


def test_immobilier_transaction_fields():
    t = immo.generate_transaction()
    assert t["prix_vente_xof"] > 0
    assert 20 <= t["duree_vente_jours"] <= 150
