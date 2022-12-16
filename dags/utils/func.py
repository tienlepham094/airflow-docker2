import logging
def log_details(*args, **kwargs):
    logging.info(f"My execution date is {kwargs['ds']}")
    logging.info(f"My execution date is {kwargs['execution_date']}")
    