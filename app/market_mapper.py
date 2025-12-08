import requests
from datetime import datetime, timezone, timedelta

# Endpoint ufficiale CLOB Get Markets (documentazione Polymarket)
# https://docs.polymarket.com/developers/CLOB/markets/get-markets
CLOB_MARKETS_URL = "https://clob.polymarket.com/markets?limit=1000"


class MarketMapper:
    """
    Mapper dai trade (conditionId) ai mercati CLOB.

    Usa:
    - condition_id       (dalla risposta CLOB)
    - question           (testo del mercato)
    - category           (Politics, Sports, Crypto, ecc.)
    - end_date_iso       (data di scadenza del mercato)
    - active / closed    (per dedurre lo stato)
    """

    def __init__(self) -> None:
        self.markets = []
        self.by_condition_id = {}

    def refresh(self) -> None:
        """
        Scarica i mercati dal CLOB e costruisce l'indice per condition_id.
        """
        try:
            resp = requests.get(CLOB_MARKETS_URL, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            # Formato secondo docs: { limit, count, next_cursor, data: [ ... ] }
            if isinstance(data, dict):
                markets = data.get("data") or data.get("markets") or []
            elif isinstance(data, list):
                markets = data
            else:
                markets = []

            self.markets = markets
            print(f"✅ MarketMapper: caricati {len(self.markets)} mercati dal CLOB")

        except Exception as e:
            print(f"❌ Errore fetch mercati CLOB: {e}")
            self.markets = []
            self.by_condition_id = {}
            return

        self.by_condition_id = {}

        for m in self.markets:
            # Nella risposta CLOB il campo è "condition_id" (snake_case)
            c_id = m.get("condition_id")
            if not c_id:
                continue

            self.by_condition_id[str(c_id).lower()] = m

    @staticmethod
    def iso_to_italian(iso_str: str) -> str:
        """
        Converte una stringa ISO (es. '2023-02-21T00:00:00Z') in
        data/ora italiana (Europa/Roma).
        """
        if not iso_str:
            return "-"

        try:
            # Esempio: 2023-02-21T00:00:00Z
            s = iso_str.replace("Z", "+00:00")
            dt_utc = datetime.fromisoformat(s)
            italy = dt_utc.astimezone(timezone(timedelta(hours=1)))
            return italy.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return iso_str

    @staticmethod
    def ts_to_italian(ts: int | float) -> str:
        """
        Converte un timestamp UNIX (secondi) in data/ora italiana.
        Se ci arrivano millisecondi, vanno già divisi PRIMA di chiamare questa funzione.
        """
        try:
            dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
            italy = dt_utc.astimezone(timezone(timedelta(hours=1)))
            return italy.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "-"

    @staticmethod
    def infer_status(market: dict) -> str:
        """
        Deduce uno stato semplice a partire da active/closed.
        """
        if not market:
            return "Unknown"

        closed = market.get("closed")
        active = market.get("active")

        if closed:
            return "closed"
        if active:
            return "open"
        return "inactive"

    def get_market_from_condition(self, condition_id: str) -> dict | None:
        """
        Trova il mercato corrispondente a una conditionId (0x... del trade).
        Il trade usa 'conditionId' (camelCase), la CLOB usa 'condition_id' (snake_case).
        """
        if not condition_id:
            return None
        key = str(condition_id).lower()
        return self.by_condition_id.get(key)
