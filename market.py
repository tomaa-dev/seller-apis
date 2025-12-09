import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Gets a list of offer-mapping-entries for the specified campaign
    from the Yandex Market Partner API.

    Args:
        page (str): Page token for page-by-page retrieval.
        campaign_id (str): сampaign/store ID in the Market.
        access_token (str): access token (Bearer) used in the Authorization header.

    Returns:
        dict: The value of the "result" field from the API JSON response.

    Raises:
        requests.exceptions.HTTPError: on HTTP errors (response.raiseforstatus()).

    Examples:
        >>> get_product_list(page, campaign_id, access_token)
        {'items': [...], ...}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Send a list of remaining stock to the Market API

    Args:
        stocks (list): List of balance records prepared for the API.
        campaign_id (list): Campaign ID in the Market.
        access_token (str): access token (Bearer) used in the Authorization header.

    Returns:
        dict: parsed JSON response from the API (response.json()).

    Raises:
        ValueError / TypeError: Possible if the input data is invalid or if the server returns an unexpected response format (e.g., not JSON).

    Examples:
        >>> updatestocks(stocks, campaign_id, access_token)
        {'result': {'processed': 1, 'errors': []}, 'requestid': '...'}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Updates offer prices in a campaign via the Yandex Market Partner API.

    Args:
        prices (list): list of price objects (offers) in the format expected by the API.
        campaign_id (str): сampaign/store ID in the Market.
        access_token (str): access token (Bearer) used in the Authorization header.

    Returns:
        dict: Parsed JSON response from the API (in the current implementation, the entire response.json() is returned)

    Raises:
        requests.exceptions.HTTPError: On HTTP error (response.raiseforstatus()).

    Examples:
        >>> update_price(prices, campaign_id, access_token)
        {'result': {'processedOffers': 2, '': []}}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Get a list of all product codes for a campaign on Yandex Market.
    
    Args:
        campaign_id (str): сampaign/store ID in the Market.
        market_token (str): api-key (token).

    Returns:
        list: A list of strings containing the SKUs (shopSku) of all found products. 
        An empty list is returned if no products are found.

    Examples:
        >>> ids = get_offer_ids("client_id", "seller_token")
        >>> isinstance(ids, list)
        True
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Generate a list of remaining items for uploading to Yandex Market.

    Args:
        watch_remnants (list/dict): List of dictionaries with residue data.
        offer_ids (list): list of items (shopSku) uploaded to the Market.
        warehouse_id (str): warehouse identifier for each entry.

    Returns:
        list of dict: list of records in the format for sending to the Market API.
        
    Raises:
        TypeError: if the passed arguments have invalid types
        ValueError: if the value in the "Quantity" field has an unexpected format and cannot be cast to int.

    Examples:
        >>> stocks = createstocks(watchremnants, offers.copy())
        [{'offerid': 'SKU1', 'stock': 100},
         {'offerid': 'SKU2', 'stock': 0},
         {'offerid': 'SKU3', 'stock': 5},
         {'offer_id': 'SKU4', 'stock': 0}]
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Generates a list of price objects to update in the API, filtering by the list of available offers.

    Args:
        watch_remnants (list/dict): List of dictionaries with residue data.
        offer_ids (list): list of items (shopSku) uploaded to the Market.

    Returns:
        list: A list of generated price objects ready to be sent to the API. 
        An empty list is returned if no matches are found.
        
    Raises:
        TypeError: if the passed arguments have invalid types

    Examples:
        >>> create_prices(watchremnants, offers.copy())
        [
            {"id": "123", "price": {"value": 499, "currencyId": "RUR"}},
            {"id": "456", "price": {"value": 299, "currencyId": "RUR"}},
        ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Asynchronously downloads updated prices for campaign offers in Yandex Market.

    Args:
        prices (list): list of price objects (offers) in the format expected by the API.
        campaign_id (str): сampaign/store ID in the Market.
        market_token (str): api-key (token).
        
    Returns:
        list: A list of all price objects generated by the createprices function.
        The returned list contains the same elements sent to the API.
        
    Raises:
        requests.exceptions.RequestException: Network communication errors occurred when calling getofferids or updateprice.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Generates and uploads balances for campaign offers to Yandex Market, 
    returns all generated records and a list of those with a non-zero inventory quantity.

    Args:
        prices (list): list of price objects (offers) in the format expected by the API.
        campaign_id (str): сampaign/store ID in the Market.
        market_token (str): api-key (token).
        warehouse_id (str): the warehouse ID that is passed to createstocks.
        
    Returns:
        tuple: notempty — a list of objects from stocks for which items[0]["count"] != 0,
stocks — a complete list of generated balance objects (even those with zero values).
        
    Raises:
        requests.exceptions.RequestException: Network communication errors occurred when calling get_offer_ids or update_price.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
