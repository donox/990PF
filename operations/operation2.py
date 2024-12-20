import logging
from exceptions.app_exceptions import InvalidInputError

logger = logging.getLogger("application")


def operation2(input_data):
    """Processes input data and returns the result."""
    try:
        if not isinstance(input_data, dict):
            raise InvalidInputError("Input data must be a dictionary.")

        logger.info("Executing operation1.")
        # Example logic
        result = {key: value + 1 for key, value in input_data.items()}
        return result
    except Exception as e:
        logger.error(f"Error in operation1: {e}")
        raise
