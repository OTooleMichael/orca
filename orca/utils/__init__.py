import uuid


def orca_id(entity_type: str) -> str:
    return f"{entity_type}_{uuid.uuid4()!s}"
