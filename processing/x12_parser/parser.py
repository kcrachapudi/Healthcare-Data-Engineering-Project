def parse_x12(message: str) -> dict:
    """
    Parses an X12 837 healthcare claim message into a structured dictionary.

    Extracts:
    - Patient name (NM1*IL)
    - Provider (NM1*82)
    - Claim info (CLM)
    - Diagnosis (HI)
    """

    data = {}

    if not message:
        return data

    # Split into segments
    segments = message.split('~')

    for seg in segments:
        seg = seg.strip()

        if not seg:
            continue

        elements = seg.split('*')
        tag = elements[0].strip()

        # -------------------------
        # PATIENT (Subscriber)
        # NM1*IL*1*LAST*FIRST
        # -------------------------
        if tag == 'NM1' and len(elements) > 4:
            entity_type = elements[1]

            if entity_type == 'IL':  # Patient
                data["patient_last_name"] = elements[3].strip()
                data["patient_first_name"] = elements[4].strip()

            elif entity_type == '82':  # Provider
                data["provider_id"] = elements[3].strip()

        # -------------------------
        # CLAIM
        # CLM*claim_id*amount
        # -------------------------
        elif tag == 'CLM' and len(elements) > 2:
            data["claim_id"] = elements[1].strip()

            try:
                data["claim_amount"] = int(elements[2].strip())
            except ValueError:
                data["claim_amount"] = 0

            # Static for now (can be improved later)
            data["claim_type"] = "professional"

        # -------------------------
        # DIAGNOSIS
        # HI*J45
        # -------------------------
        elif tag == 'HI' and len(elements) > 1:
            data["diagnosis_code"] = elements[1].strip()

    return data