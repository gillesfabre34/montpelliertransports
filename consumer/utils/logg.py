from rich import print


def logg(title: str, value=None) -> None:
    if value is None:
        print(f"\n----------\n{title}\n----------\n")
    else:
        print(f"\n----------\n{title}\n----------\n", value)
