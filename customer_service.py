
import requests
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://example.com/api/customers"

def fetch_customers():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error("Error fetching customers: %s", e)
        return None

def log_first_customer(customers):
    if not customers:
        logger.warning("No customers found")
        return

    first_customer = customers[0]
    logger.info("First customer object: %s", first_customer)

    firstname = first_customer.get("firstname")
    if firstname:
        logger.info("First customer's firstname: %s", firstname)
    else:
        logger.warning("Firstname not found in the first customer object.")

if __name__ == "__main__":
    customers = fetch_customers()
    if customers is not None:
        log_first_customer(customers)
