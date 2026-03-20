def parse_x12(x12_message):
    segments = x12_message.strip().split("~")
    data = {}

    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        elements = segment.split("*")
        segment_id = elements[0]

        # Patient (Subscriber)
        if segment_id == "NM1" and len(elements) >= 5:
            if elements[1] == "IL":
                data["patient_last_name"] = elements[3]
                data["patient_first_name"] = elements[4]

        # Claim
        elif segment_id == "CLM" and len(elements) >= 3:
            data["claim_id"] = elements[1]
            data["claim_amount"] = elements[2]

    return data