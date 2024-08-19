import logging
import logging.config

def setup_query_logging():
    QUERY_LEVEL_NUM = 25  # Custom level number
    logging.addLevelName(QUERY_LEVEL_NUM, "QUERY")
        
    def query(self, message, *args, **kwargs):
        # Add the custom log method to the Logger class
        if self.isEnabledFor(QUERY_LEVEL_NUM):
            self._log(QUERY_LEVEL_NUM, message, args, **kwargs)
            
    logging.Logger.query = query
    
def setup_logging(level="INFO"):
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "verbose": {
                "format": "{levelname} {asctime} {module} {message}",
                "style": "{",
            },
            "simple": {
                "format": "{levelname} {message}",
                "style": "{",
            },
        },
        "handlers": {
            "file": {
                "level": "DEBUG",
                "class": "logging.FileHandler",
                "filename": "app.log",
                "formatter": "verbose",
            },
            "console": {
                "level": level,
                "class": "logging.StreamHandler",
                "formatter": "simple",
            },
        },
        "loggers": {
            "": {  # Root logger
                "handlers": ["file", "console"],
                "level": "DEBUG",
                "propagate": True,
            },
        },
    })

setup_query_logging()
setup_logging()
