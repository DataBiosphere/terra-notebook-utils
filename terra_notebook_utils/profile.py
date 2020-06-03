"""
Miscelenious user profile commands
"""
from firecloud import fiss


def list_billing_projects() -> list:
    """
    Billing projects available to the current usuer
    """
    resp = fiss.fapi.list_billing_projects()
    resp.raise_for_status()
    return resp.json()
