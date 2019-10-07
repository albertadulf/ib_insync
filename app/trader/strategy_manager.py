from app.trader.follow_strategy import FollowStrategy
from app.trader.mean_strategy import MeanStrategy
from app.trader.medium_strategy import MediumStrategy
from app.trader.random_strategy import RandomStrategy

Strategies = {
    'follow': FollowStrategy,
    'mean': MeanStrategy,
    'medium': MediumStrategy,
    'random': RandomStrategy,
}
