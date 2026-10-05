"""Creates a small set of test cards in different states, so the network
simulator has realistic accounts to drive traffic against. Writes the
tokens to scripts/cards.json (gitignored) for the simulator to read."""
import json
import uuid

import psycopg

DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/cards"


def make_funded_card(conn, balance_minor, status="active"):
    account_id = conn.execute(
        "INSERT INTO accounts(account_type) VALUES ('customer') RETURNING account_id"
    ).fetchone()[0]

    if balance_minor:
        suspense_id = conn.execute(
            "INSERT INTO accounts(account_type) VALUES ('suspense') RETURNING account_id"
        ).fetchone()[0]
        entry_id = conn.execute(
            "INSERT INTO journal_entries(entry_type, transaction_id, idempotency_key) "
            "VALUES ('adjustment', %s, %s) RETURNING entry_id",
            (uuid.uuid4(), f"seed-{uuid.uuid4()}"),
        ).fetchone()[0]
        conn.execute("INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
                     (entry_id, suspense_id, -balance_minor))
        conn.execute("INSERT INTO journal_lines(entry_id, account_id, amount_minor) VALUES (%s,%s,%s)",
                     (entry_id, account_id, balance_minor))

    card_token = uuid.uuid4()
    conn.execute("INSERT INTO card_vault(card_token, pan_encrypted, pan_last4) VALUES (%s,%s,%s)",
                 (card_token, b"fake", "4242"))
    conn.execute(
        "INSERT INTO cards(card_token, account_id, card_type, status, expires_on) "
        "VALUES (%s,%s,'virtual',%s,'2027-01-01')",
        (card_token, account_id, status),
    )
    return str(card_token), account_id


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=0,
                        help="also create a pool of N well-funded cards in scripts/pool.json")
    args = parser.parse_args()

    conn = psycopg.connect(DATABASE_URL)
    cards = {
        "well_funded": make_funded_card(conn, balance_minor=5_000_000)[0],
        "low_balance": make_funded_card(conn, balance_minor=500)[0],
        "frozen": make_funded_card(conn, balance_minor=5_000_000, status="frozen")[0],
    }
    pool = [make_funded_card(conn, balance_minor=5_000_000)[0] for _ in range(args.count)]
    conn.commit()

    with open("scripts/cards.json", "w") as f:
        json.dump(cards, f, indent=2)
    if args.count:
        with open("scripts/pool.json", "w") as f:
            json.dump({"pool": pool, "low_balance": cards["low_balance"], "frozen": cards["frozen"]}, f, indent=2)
        print(f"created {args.count} pool cards in scripts/pool.json")
    for name, token in cards.items():
        print(f"{name}: {token}")
