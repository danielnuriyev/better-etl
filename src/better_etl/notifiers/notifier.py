import logging

_logger = logging.getLogger(__name__)

class Notifier:

    def __init__(self, **kwargs):
        _logger.info(kwargs)

    def notify(self, message):

        _logger.info(message)
