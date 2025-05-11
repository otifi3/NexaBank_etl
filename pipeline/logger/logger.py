import logging

class Logger:
    def __init__(self, log_file: str) -> None:
        """
        Initializes the logger.

        :param log_file: Path to the log file where logs will be saved.
        """
        self.log_file = log_file

        # Set up logging configuration
        logging.basicConfig(filename=self.log_file, 
                            level=logging.DEBUG, 
                            format='%(asctime)s [%(levelname)s] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def log(self, level: str, msg: str) -> None:
        """
        Log a message with the given level.

        :param level: The level of the log ('info', 'error', 'warning')
        :param msg: The message to log.
        """
        if level.lower() == 'error':
            logging.error(msg)
        elif level.lower() == 'info':
            logging.info(msg)
        elif level.lower() == 'warning':
            logging.warning(msg)
