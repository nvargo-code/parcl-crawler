"""Address normalization for parcl-crawler (v1: string-based)."""

from __future__ import annotations

import re

# Common street suffix abbreviations (USPS Publication 28)
SUFFIX_MAP = {
    "STREET": "ST",
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "DRIVE": "DR",
    "LANE": "LN",
    "ROAD": "RD",
    "COURT": "CT",
    "CIRCLE": "CIR",
    "PLACE": "PL",
    "TRAIL": "TRL",
    "PARKWAY": "PKWY",
    "HIGHWAY": "HWY",
    "EXPRESSWAY": "EXPY",
    "TERRACE": "TER",
    "WAY": "WAY",
    "COVE": "CV",
    "LOOP": "LOOP",
    "PASS": "PASS",
    "PATH": "PATH",
    "RUN": "RUN",
    "CROSSING": "XING",
}

DIRECTIONAL_MAP = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
}

UNIT_MAP = {
    "APARTMENT": "APT",
    "SUITE": "STE",
    "UNIT": "UNIT",
    "BUILDING": "BLDG",
    "FLOOR": "FL",
    "ROOM": "RM",
    "NUMBER": "#",
    "NO": "#",
    "NO.": "#",
    "#": "#",
}


def normalize_address(address: str | None) -> str:
    """Normalize an address string for matching.

    v1 approach: uppercase, strip punctuation, expand/abbreviate suffixes,
    collapse whitespace. Future: USPS CASS or geocoding API.
    """
    if not address:
        return ""

    addr = address.upper().strip()

    # Remove punctuation except # (unit numbers)
    addr = re.sub(r"[.,;:!?'\"\(\)\[\]]", "", addr)

    # Replace directionals
    words = addr.split()
    normalized = []
    for word in words:
        if word in DIRECTIONAL_MAP:
            normalized.append(DIRECTIONAL_MAP[word])
        elif word in SUFFIX_MAP:
            normalized.append(SUFFIX_MAP[word])
        elif word in UNIT_MAP:
            normalized.append(UNIT_MAP[word])
        else:
            normalized.append(word)

    # Collapse whitespace
    result = " ".join(normalized)
    # Remove duplicate spaces
    result = re.sub(r"\s+", " ", result).strip()
    return result
