from eventkit import Event


class ServerEvents:
    def __init__(self):
        self.market_data = Event('marketDataEvent')
