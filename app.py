"""
Databricks App boilerplate:
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py
- Pulls data from the Massive API via massive_client.py and syncs it into Lakebase

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
import re

import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase
from massive_client import MassiveClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("massive-app")

app = Flask(__name__)
_w = WorkspaceClient()

TABLE_NAME = os.environ.get("MASSIVE_TABLE_NAME", "massive_records")
WATCHLIST_TABLE_NAME = os.environ.get("WATCHLIST_TABLE_NAME", "watchlist")

# Basic stock ticker shape check: 1-10 uppercase letters, with an optional
# ".X" or ".XX" share-class suffix (e.g. "BRK.B"). This rejects obviously
# malformed input before we even call the Massive API.
_TICKER_RE = re.compile(r"^[A-Z]{1,10}(\.[A-Z]{1,2})?$")


def ensure_table():
    """Create the destination table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def ensure_watchlist_table():
    """Create the watchlist table in Lakebase if it doesn't exist yet."""
    # Create the base table if it doesn't exist
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WATCHLIST_TABLE_NAME} (
            symbol TEXT NOT NULL,
            email TEXT NOT NULL,
            latest_price NUMERIC,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, email)
        )
        """
    )
    
    # Migrate existing table by adding new columns if they don't exist
    _migrate_watchlist_table()


def _migrate_watchlist_table():
    """Add new columns to the watchlist table if they don't exist yet."""
    new_columns = [
        ("company_name", "TEXT"),
        ("description", "TEXT"),
        ("market_cap", "NUMERIC"),
        ("sector", "TEXT"),
        ("industry", "TEXT"),
        ("logo_url", "TEXT"),
        ("day_high", "NUMERIC"),
        ("day_low", "NUMERIC"),
        ("volume", "BIGINT"),
        ("percent_change", "NUMERIC"),
    ]
    
    for column_name, column_type in new_columns:
        try:
            lakebase.run_write(
                f"ALTER TABLE {WATCHLIST_TABLE_NAME} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            )
        except Exception as e:
            # Some Postgres versions don't support IF NOT EXISTS in ALTER TABLE,
            # so we catch the exception if the column already exists
            if "already exists" not in str(e).lower():
                logger.warning(f"Could not add column {column_name}: {e}")


def _current_user_email() -> str:
    """
    Resolve the current user's email so the watchlist can be personalized.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Simple UI to submit a list of stock symbols to sync from Massive."""
    return render_template("index.html")


@app.route("/records")
def list_records():
    """Read records already synced into Lakebase."""
    limit = int(request.args.get("limit", 100))
    rows = lakebase.run_query(
        f"SELECT id, payload, synced_at FROM {TABLE_NAME} ORDER BY synced_at DESC LIMIT %s",
        (limit,),
    )
    return jsonify(rows)


@app.route("/sync", methods=["POST"])
def sync_from_massive():
    """
    Pull data from the Massive API (paginated, potentially huge dataset) and
    upsert it into Lakebase in batches.
    """
    ensure_table()
    client = MassiveClient()

    path = request.json.get("path", "/records") if request.is_json else "/records"
    batch_size = int(request.args.get("batch_size", 500))

    batch = []
    total = 0
    for item in client.paginated_get(path):
        batch.append(item)
        if len(batch) >= batch_size:
            total += _upsert_batch(batch)
            batch = []

    if batch:
        total += _upsert_batch(batch)

    return jsonify({"synced": total})


@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    """Return the current user's watchlist symbols with all rich data fields."""
    ensure_watchlist_table()
    email = _current_user_email()
    rows = lakebase.run_query(
        f"""
        SELECT 
            symbol, email, latest_price, company_name, description,
            market_cap, sector, industry, logo_url, day_high, day_low,
            volume, percent_change, updated_at
        FROM {WATCHLIST_TABLE_NAME}
        WHERE email = %s 
        ORDER BY symbol ASC
        """,
        (email,),
    )
    return jsonify(rows)


@app.route("/watchlist/<symbol>", methods=["DELETE"])
def delete_from_watchlist(symbol):
    """
    Remove a symbol from the current user's watchlist.
    """
    ensure_watchlist_table()
    email = _current_user_email()
    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""
    
    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400
    
    lakebase.run_write(
        f"""
        DELETE FROM {WATCHLIST_TABLE_NAME}
        WHERE symbol = %s AND email = %s
        """,
        (symbol, email),
    )
    
    return jsonify({"symbol": symbol, "deleted": True})


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    """
    Fetch the latest price AND comprehensive ticker details for a single stock
    symbol from Massive, then add/update that symbol on the watchlist in Lakebase
    with all rich data fields.
    """
    ensure_watchlist_table()

    if request.is_json:
        symbol = request.json.get("symbol", "")
    else:
        symbol = request.form.get("symbol", "")

    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""

    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    client = MassiveClient()
    
    # Fetch latest price data
    try:
        price_data = client.get_latest_price(symbol)
    except requests.HTTPError:
        return jsonify({"error": f"Unknown ticker symbol: {symbol}"}), 400

    price = _extract_latest_price(price_data)
    if price is None:
        return jsonify({"error": f"No price data available for ticker: {symbol}"}), 400

    # Extract additional price fields (high, low, volume, percent change)
    price_details = _extract_price_details(price_data)
    
    # Fetch comprehensive ticker details (company info, fundamentals)
    ticker_details = {}
    try:
        details_data = client.get_ticker_details(symbol)
        ticker_details = _extract_ticker_details(details_data)
    except requests.HTTPError as e:
        # If ticker details fail, log but continue with just price data
        logger.warning(f"Could not fetch ticker details for {symbol}: {e}")

    email = _current_user_email()

    # Combine all data for insertion
    lakebase.run_write(
        f"""
        INSERT INTO {WATCHLIST_TABLE_NAME} (
            symbol, email, latest_price, company_name, description,
            market_cap, sector, industry, logo_url, day_high, day_low,
            volume, percent_change, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (symbol, email) DO UPDATE
            SET latest_price = EXCLUDED.latest_price,
                company_name = EXCLUDED.company_name,
                description = EXCLUDED.description,
                market_cap = EXCLUDED.market_cap,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                logo_url = EXCLUDED.logo_url,
                day_high = EXCLUDED.day_high,
                day_low = EXCLUDED.day_low,
                volume = EXCLUDED.volume,
                percent_change = EXCLUDED.percent_change,
                updated_at = EXCLUDED.updated_at
        """,
        (
            symbol,
            email,
            price,
            ticker_details.get("company_name"),
            ticker_details.get("description"),
            ticker_details.get("market_cap"),
            ticker_details.get("sector"),
            ticker_details.get("industry"),
            ticker_details.get("logo_url"),
            price_details.get("day_high"),
            price_details.get("day_low"),
            price_details.get("volume"),
            price_details.get("percent_change"),
        ),
    )

    return jsonify({
        "symbol": symbol,
        "email": email,
        "latest_price": price,
        **ticker_details,
        **price_details,
    })


def _extract_latest_price(data: dict) -> float | None:
    """Pull the trade price out of the Massive 'previous close' response shape.

    The /v2/aggs/ticker/{symbol}/prev endpoint returns "results" as a LIST
    containing a single aggregate bar (not a dict), e.g.:
        {"status": "OK", "resultsCount": 1, "results": [{"c": 148.845, ...}]}
    Previously this code treated "results" as a dict, so isinstance(results, dict)
    was always False for this endpoint's real shape and the price silently
    resolved to None. Unwrap the list here, and check "status"/"resultsCount"
    so invalid tickers (empty results) are detected instead of "succeeding"
    with a null price.

    Adjust the key lookup here if the real Massive API returns a different
    field name for the traded/close price.
    """
    if not isinstance(data, dict):
        return None
    if data.get("status") not in (None, "OK") or data.get("resultsCount") == 0:
        return None
    results = data.get("results", data)
    if isinstance(results, list):
        results = results[0] if results else None
    if isinstance(results, dict):
        for key in ("c", "p", "price", "last_price", "vw"):
            if key in results:
                return results[key]
    return None


def _extract_price_details(data: dict) -> dict:
    """Extract additional price fields from the /prev endpoint response.
    
    Returns a dict with day_high, day_low, volume, and percent_change.
    The /v2/aggs/ticker/{symbol}/prev response shape:
        {"status": "OK", "results": [{"h": high, "l": low, "v": volume, ...}]}
    """
    details = {
        "day_high": None,
        "day_low": None,
        "volume": None,
        "percent_change": None,
    }
    
    if not isinstance(data, dict):
        return details
    
    results = data.get("results", [])
    if isinstance(results, list) and results:
        result = results[0]
        if isinstance(result, dict):
            details["day_high"] = result.get("h")
            details["day_low"] = result.get("l")
            details["volume"] = result.get("v")
            
            # Calculate percent change if open and close are available
            open_price = result.get("o")
            close_price = result.get("c")
            if open_price and close_price and open_price > 0:
                details["percent_change"] = ((close_price - open_price) / open_price) * 100
    
    return details


def _extract_ticker_details(data: dict) -> dict:
    """Extract company fundamentals from the /v3/reference/tickers/{symbol} response.
    
    Returns a dict with company_name, description, market_cap, sector, industry, logo_url.
    The API response shape:
        {"status": "OK", "results": {"name": "...", "market_cap": ..., ...}}
    """
    details = {
        "company_name": None,
        "description": None,
        "market_cap": None,
        "sector": None,
        "industry": None,
        "logo_url": None,
    }
    
    if not isinstance(data, dict):
        return details
    
    results = data.get("results", {})
    if isinstance(results, dict):
        details["company_name"] = results.get("name")
        details["description"] = results.get("description")
        details["market_cap"] = results.get("market_cap")
        
        # Sector and industry might be nested or at top level depending on API version
        details["sector"] = results.get("sector") or results.get("sic_description")
        details["industry"] = results.get("industry") or results.get("sic_code")
        
        # Logo URL might be under branding
        branding = results.get("branding", {})
        if isinstance(branding, dict):
            details["logo_url"] = branding.get("logo_url") or branding.get("icon_url")
    
    return details


def _upsert_batch(items: list[dict]) -> int:
    """Upsert a batch of Massive API items into Lakebase, one statement per row.

    For very large batches, consider psycopg2.extras.execute_values for
    higher throughput instead of per-row execute calls.
    """
    import json as _json

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (id, payload, synced_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                        SET payload = EXCLUDED.payload,
                            synced_at = EXCLUDED.synced_at
                    """,
                    (str(item.get("id")), _json.dumps(item)),
                )
                count += 1
            conn.commit()
    return count


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")