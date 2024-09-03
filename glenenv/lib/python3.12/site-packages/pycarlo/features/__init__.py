from random import randrange


def downtime() -> None:
    """
    A bit of fun. Demos SDK feature extensions.
    """
    if randrange(10) >= 5:
        raise SystemExit("Bad luck.")
    print("No data downtime found.")
