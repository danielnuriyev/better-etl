import logging

_logger = logging.getLogger(__init__)

class Notifier:

    def notify(self, message):

        _logger.info(message)
