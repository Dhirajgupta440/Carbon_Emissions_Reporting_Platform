from __future__ import annotations

from datetime import date

from sqlalchemy import or_
from sqlalchemy.orm import Session

from .models import BusinessMetric, EmissionFactor, EmissionRecord


def get_valid_emission_factor(
    db: Session,
    scope: str,
    activity_name: str,
    unit: str,
    activity_date: date,
) -> EmissionFactor | None:
    return (
        db.query(EmissionFactor)
        .filter(
            EmissionFactor.scope == scope,
            EmissionFactor.activity_name == activity_name,
            EmissionFactor.activity_unit == unit,
            EmissionFactor.valid_from <= activity_date,
            or_(EmissionFactor.valid_to.is_(None), EmissionFactor.valid_to >= activity_date),
        )
        .order_by(EmissionFactor.valid_from.desc(), EmissionFactor.id.desc())
        .first()
    )


def calculate_emission_kg(quantity: float, factor_value: float) -> float:
    return round(quantity * factor_value, 4)


def seed_sample_data(db: Session) -> None:
    if db.query(EmissionFactor).count() == 0:
        db.add_all(
            [
                EmissionFactor(
                    scope="Scope 1",
                    category="Mobile Combustion",
                    activity_name="Diesel",
                    activity_unit="liters",
                    co2e_kg_per_unit=2.6800,
                    factor_source="Sample DEFRA aligned dataset",
                    version_label="2024-v1",
                    valid_from=date(2024, 1, 1),
                    valid_to=date(2024, 12, 31),
                ),
                EmissionFactor(
                    scope="Scope 1",
                    category="Mobile Combustion",
                    activity_name="Diesel",
                    activity_unit="liters",
                    co2e_kg_per_unit=2.6600,
                    factor_source="Sample DEFRA aligned dataset",
                    version_label="2025-v1",
                    valid_from=date(2025, 1, 1),
                    valid_to=date(2025, 12, 31),
                ),
                EmissionFactor(
                    scope="Scope 1",
                    category="Mobile Combustion",
                    activity_name="Diesel",
                    activity_unit="liters",
                    co2e_kg_per_unit=2.6400,
                    factor_source="Sample DEFRA aligned dataset",
                    version_label="2026-v1",
                    valid_from=date(2026, 1, 1),
                    valid_to=None,
                ),
                EmissionFactor(
                    scope="Scope 1",
                    category="Stationary Combustion",
                    activity_name="LPG",
                    activity_unit="kg",
                    co2e_kg_per_unit=3.0100,
                    factor_source="Sample EPA stationary fuel factor",
                    version_label="2024-v1",
                    valid_from=date(2024, 1, 1),
                    valid_to=date(2024, 12, 31),
                ),
                EmissionFactor(
                    scope="Scope 1",
                    category="Stationary Combustion",
                    activity_name="LPG",
                    activity_unit="kg",
                    co2e_kg_per_unit=2.9800,
                    factor_source="Sample EPA stationary fuel factor",
                    version_label="2025-v1",
                    valid_from=date(2025, 1, 1),
                    valid_to=date(2025, 12, 31),
                ),
                EmissionFactor(
                    scope="Scope 1",
                    category="Stationary Combustion",
                    activity_name="LPG",
                    activity_unit="kg",
                    co2e_kg_per_unit=2.9500,
                    factor_source="Sample EPA stationary fuel factor",
                    version_label="2026-v1",
                    valid_from=date(2026, 1, 1),
                    valid_to=None,
                ),
                EmissionFactor(
                    scope="Scope 2",
                    category="Purchased Electricity",
                    activity_name="Grid Electricity",
                    activity_unit="kWh",
                    co2e_kg_per_unit=0.8200,
                    factor_source="Sample grid electricity factor",
                    version_label="2024-v1",
                    valid_from=date(2024, 1, 1),
                    valid_to=date(2024, 12, 31),
                ),
                EmissionFactor(
                    scope="Scope 2",
                    category="Purchased Electricity",
                    activity_name="Grid Electricity",
                    activity_unit="kWh",
                    co2e_kg_per_unit=0.7900,
                    factor_source="Sample grid electricity factor",
                    version_label="2025-v1",
                    valid_from=date(2025, 1, 1),
                    valid_to=date(2025, 12, 31),
                ),
                EmissionFactor(
                    scope="Scope 2",
                    category="Purchased Electricity",
                    activity_name="Grid Electricity",
                    activity_unit="kWh",
                    co2e_kg_per_unit=0.7500,
                    factor_source="Sample grid electricity factor",
                    version_label="2026-v1",
                    valid_from=date(2026, 1, 1),
                    valid_to=None,
                ),
            ]
        )
        db.commit()

    if db.query(BusinessMetric).count() == 0:
        db.add_all(
            [
                BusinessMetric(metric_date=date(2025, 1, 31), metric_name="Tons of Steel Produced", metric_unit="tons", value=4800),
                BusinessMetric(metric_date=date(2025, 2, 28), metric_name="Tons of Steel Produced", metric_unit="tons", value=4950),
                BusinessMetric(metric_date=date(2025, 3, 31), metric_name="Tons of Steel Produced", metric_unit="tons", value=5100),
                BusinessMetric(metric_date=date(2025, 4, 30), metric_name="Tons of Steel Produced", metric_unit="tons", value=5200),
                BusinessMetric(metric_date=date(2026, 1, 31), metric_name="Tons of Steel Produced", metric_unit="tons", value=5300),
                BusinessMetric(metric_date=date(2026, 2, 28), metric_name="Tons of Steel Produced", metric_unit="tons", value=5450),
                BusinessMetric(metric_date=date(2026, 3, 31), metric_name="Tons of Steel Produced", metric_unit="tons", value=5600),
                BusinessMetric(metric_date=date(2026, 4, 30), metric_name="Tons of Steel Produced", metric_unit="tons", value=5700),
                BusinessMetric(metric_date=date(2025, 12, 31), metric_name="Employees", metric_unit="employees", value=1120),
                BusinessMetric(metric_date=date(2026, 3, 31), metric_name="Employees", metric_unit="employees", value=1155),
            ]
        )
        db.commit()

    if db.query(EmissionRecord).count() == 0:
        sample_records = [
            ("Scope 1", "Mobile Combustion", "Diesel", 12000, "liters", date(2025, 1, 15), "Fleet fuel consumption"),
            ("Scope 1", "Stationary Combustion", "LPG", 1800, "kg", date(2025, 2, 10), "Boiler usage"),
            ("Scope 2", "Purchased Electricity", "Grid Electricity", 85000, "kWh", date(2025, 3, 20), "Plant electricity"),
            ("Scope 1", "Mobile Combustion", "Diesel", 13400, "liters", date(2025, 4, 18), "Logistics operations"),
            ("Scope 2", "Purchased Electricity", "Grid Electricity", 91000, "kWh", date(2025, 5, 20), "Plant electricity"),
            ("Scope 1", "Stationary Combustion", "LPG", 1650, "kg", date(2025, 6, 8), "Heat treatment"),
            ("Scope 1", "Mobile Combustion", "Diesel", 12500, "liters", date(2026, 1, 16), "Fleet fuel consumption"),
            ("Scope 2", "Purchased Electricity", "Grid Electricity", 88000, "kWh", date(2026, 1, 25), "Plant electricity"),
            ("Scope 1", "Stationary Combustion", "LPG", 1900, "kg", date(2026, 2, 11), "Boiler usage"),
            ("Scope 2", "Purchased Electricity", "Grid Electricity", 94000, "kWh", date(2026, 2, 23), "Plant electricity"),
            ("Scope 1", "Mobile Combustion", "Diesel", 14200, "liters", date(2026, 3, 14), "Outbound transport"),
            ("Scope 2", "Purchased Electricity", "Grid Electricity", 97000, "kWh", date(2026, 3, 27), "Plant electricity"),
            ("Scope 1", "Mobile Combustion", "Diesel", 13800, "liters", date(2026, 4, 9), "Inbound logistics"),
            ("Scope 2", "Purchased Electricity", "Grid Electricity", 99000, "kWh", date(2026, 4, 21), "Plant electricity"),
        ]

        for scope, category, activity_name, quantity, unit, activity_date, notes in sample_records:
            factor = get_valid_emission_factor(db, scope, activity_name, unit, activity_date)
            if factor is None:
                continue

            emissions = calculate_emission_kg(quantity, factor.co2e_kg_per_unit)
            db.add(
                EmissionRecord(
                    scope=scope,
                    category=category,
                    activity_name=activity_name,
                    quantity=quantity,
                    unit=unit,
                    activity_date=activity_date,
                    calculated_kg_co2e=emissions,
                    final_kg_co2e=emissions,
                    notes=notes,
                    emission_factor_id=factor.id,
                )
            )

        db.commit()
