from pydantic import StrictFloat, StrictStr


def parse_latency(latency: StrictStr) -> StrictFloat:
    """
    Parses a latency string in the format "Xs" to an integer.
    """
    if "s" in latency:
        return float(latency[:-1])
    else:
        raise ValueError(f"Invalid latency format: {latency}")
