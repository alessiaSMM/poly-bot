import requests
from datetime import datetime, timezone, timedelta

GAMMA_URL = "https://gamma-api.polymarket.com/markets"

class MarketMapper:
    """
    Scarica e indicizza tutti i mercati Polymarket.
    Consente lookup tramite marketId o conditionId.
    """

    def __init__(self):
        self.markets = []
        self.by_market_id = {}
        self.by_condition_id = {}

    def refresh(self):
        """
        Scarica tutti i mercati Polymarket e costruisce gli indici.
        """
        try:
            r = requests.get(GAMMA_URL, timeout=10)
            r.raise_for_status()
            self.markets = r.json()
        except Exception as e:
            print("❌ Errore fetch mercati:", e)
            return

        self.by_market_id.clear()
        self.by_condition_id.clear()

        for m in self.markets:
            market_id = m.get("id")
            cond_id = m.get("conditionId")

            if market_id:
                self.by_market_id[str(market_id)] = m
            if cond_id:
                self.by_condition_id[cond_id.lower()] = m

    @staticmethod
    def ts_to_italian(ts):
        """
        Converte UNIX timestamp (secondi) → data/ora italiana (CET/CEST).
        """
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        italy = dt_utc.astimezone(timezone(timedelta(hours=1)))
        return italy.strftime("%Y-%m-%d %H:%M:%S")

    def get_market_from_trade(self, trade):
        """
        Trova il mercato corretto a partire da un trade della Data API.
        Prova prima marketId, poi conditionId.
        """

        # 1. marketId (se presente)
        m_id = trade.get("marketId")
        if m_id and str(m_id) in self.by_market_id:
            return self.by_market_id[str(m_id)]

        # 2. conditionId
        c_id = trade.get("conditionId")
        if c_id:
            c_id = c_id.lower()
            if c_id in self.by_condition_id:
                return self.by_condition_id[c_id]

        return None
