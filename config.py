"""
County configuration registry for the property tax appraisal protest tool.
Maps county names to their data source configuration, API parameters, and display settings.
"""

COUNTIES = {
    "Travis": {
        "type": "api",
        "display_name": "Travis Central Appraisal District (TCAD)",
        "short_name": "TCAD",
        "db_file": "tcad_data.db",
        "scraped_data_dir": "scraped_data/travis",
        "property_link_base": "https://travis.prodigycad.com/property-detail",
        "link_format": "{base}/{pid}/{year}",
        "has_parcel_data": True,
        "has_pdf_cards": True,
        # TrueProdigy API config
        "api_base": "https://prod-container.trueprodigyapi.com",
        "office": "Travis",
        "origin": "https://travis.prodigycad.com",
        "tp_database": "travis_appraisal",
        "tp_office_name": "travis",
    },
    "Williamson": {
        "type": "html",
        "display_name": "Williamson Central Appraisal District (WCAD)",
        "short_name": "WCAD",
        "db_file": "wcad_data.db",
        "scraped_data_dir": "scraped_data/williamson",
        "property_link_base": "https://search.wcad.org/Property-Detail/PropertyQuickRefID",
        "link_format": "{base}/{property_id}/PartyQuickRefID/{party_id}/SearchTaxYear/{year}",
        "has_parcel_data": False,
        "has_pdf_cards": False,
        # WCAD scraping config
        "search_url": "https://search.wcad.org/ProxyT/Search/Properties/advancedsearch",
        "detail_url_template": "https://search.wcad.org/Property-Detail/PropertyQuickRefID/{property_id}/PartyQuickRefID/{party_id}/SearchTaxYear/{year}",
    },
}

# Default tax year - will be derived from data when possible
DEFAULT_YEAR = "2026"


def get_county_config(county_name):
    """Get configuration for a county by name (case-insensitive)."""
    for name, config in COUNTIES.items():
        if name.lower() == county_name.lower():
            return config
    raise ValueError(f"Unknown county: {county_name}. Available: {list(COUNTIES.keys())}")
