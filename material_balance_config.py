# material_balance_config.py
"""
Material Balance Configuration for different locations
Maps OTR operation names to Material Balance columns (location-aware)

CRITICAL:
- Table column headers below MUST match exactly what you want to see on the page/export.
- OTR operation names on the RIGHT side must match exactly what is saved in OTRRecord.operation.value.
"""

class MaterialBalanceConfig:
    """Configuration for location-specific material balance calculations"""

    # ----------------------------------------------------------------------
    # Exact column order (and casing) per location — DO NOT CHANGE ORDER
    # ----------------------------------------------------------------------
    LOCATION_COLUMNS = {
        "AGGU": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt",
                "Dispatch",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                # Column label -> list/str of OTR operation names
                "Receipt": "Receipt",
            },
            "dispatch_operations": {
                "Dispatch": "Dispatch",
            },
            "name": "Aggu",
        },

        "AGGE": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt",
                "Dispatch",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                "Receipt": "Receipt",
            },
            "dispatch_operations": {
                "Dispatch": "Dispatch",
            },
            "name": "Agge",
        },

        # Asemoku Jetty — note the exact header casing you requested
        "JETTY": {
            "columns": [
                "Date",
                "Opening stock",     # <- lower 's'
                "OKW Receipt",
                "ANZ receipt",       # <- lower 'r'
                "Other receipts",    # <- lower 'r'
                "Dispatch to barge",
                "Other dispatch",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                # OTR ops (exact strings in OTR) → shown under these column labels
                "OKW Receipt":    "OKW Receipt",
                "ANZ receipt":    "ANZ Receipt",   # column label lower 'r', OTR op likely "ANZ Receipt"
                "Other receipts": "Other Receipts"
            },
            "dispatch_operations": {
                "Dispatch to barge": "Dispatch to barge",
                "Other dispatch":    "Other Dispatch",
            },
            "name": "Asemoku Jetty",
        },

        # Beneku (BFS)
        "BFS": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt-Commingled",
                "Receipt-Condensate",
                "Dispatch to Jetty",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                "Receipt-Commingled":  "Receipt - Commingled",
                "Receipt-Condensate":  "Receipt - Condensate",  # special: use NSV (see special_handling)
            },
            "dispatch_operations": {
                "Dispatch to Jetty": "Dispatch to Jetty",
            },
            "name": "Beneku (BFS)",
            "special_handling": {
                # tells calculator to use NSV instead of qty_bbls for this column
                "Receipt-Condensate": "use_nsv"
            },
        },

        "NDONI": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt from Agu",
                "Receipt from OFS",
                "Other Receipts",
                "Dispatch to barge",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                "Receipt from Agu": "Receipt from Agu",
                "Receipt from OFS": "Receipt from OFS",
                "Other Receipts":   "Other Receipts",
            },
            "dispatch_operations": {
                "Dispatch to barge": "Dispatch to barge",
            },
            "name": "Ndoni",
        },

        # Utapate (OML-13)
        "OML-13": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt",
                "Dispatch",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                "Receipt": "Receipt",
            },
            "dispatch_operations": {
                "Dispatch": "Dispatch",
            },
            "name": "Utapate",
        },

        # Ogini (OML-26)
        "OML-26": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt",
                "Dispatch",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                "Receipt": "Receipt",
            },
            "dispatch_operations": {
                "Dispatch": "Dispatch",
            },
            "name": "Ogini",
        },

        # Oguali (OML-157)
        "OML-157": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt",
                "Dispatch",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                "Receipt": "Receipt",
            },
            "dispatch_operations": {
                "Dispatch": "Dispatch",
            },
            "name": "Oguali",
        },

        # Lagos (HO)
        "HO": {
            "columns": [
                "Date",
                "Opening Stock",
                "Receipt",
                "Dispatch",
                "Book closing stock",
                "Closing stock",
                "Loss/Gain",
            ],
            "receipt_operations": {
                "Receipt": "Receipt",
            },
            "dispatch_operations": {
                "Dispatch": "Dispatch",
            },
            "name": "Lagos (HO)",
        },
    }

    # ----------------------------
    # Convenience accessors below
    # ----------------------------
    @classmethod
    def get_config(cls, location_code):
        if not location_code:
            return None
        return cls.LOCATION_COLUMNS.get(str(location_code).upper(), None)

    @classmethod
    def get_columns(cls, location_code):
        cfg = cls.get_config(location_code)
        return list(cfg["columns"]) if cfg else []

    @classmethod
    def get_receipt_operations(cls, location_code):
        cfg = cls.get_config(location_code)
        return dict(cfg["receipt_operations"]) if cfg else {}

    @classmethod
    def get_dispatch_operations(cls, location_code):
        cfg = cls.get_config(location_code)
        return dict(cfg["dispatch_operations"]) if cfg else {}

    @classmethod
    def get_special_handling(cls, location_code):
        cfg = cls.get_config(location_code)
        return dict(cfg.get("special_handling", {})) if cfg else {}

    @classmethod
    def get_location_name(cls, location_code):
        cfg = cls.get_config(location_code)
        return cfg["name"] if cfg else str(location_code)
