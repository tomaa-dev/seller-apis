import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Receives a list of products according to the set parameters.

    Args:
        last_id (str): last element identifier.
        client_id (str): unique client number for identification in the system.
        seller_token (str): api-key (token) of the seller.

    Returns:
         list: a list of products.
         
    Examples:
        >>> get_product_list("", "01234567-89ab-cdef-0123-456789abcdef", "api-key")
         [{'product_id': 12345, 'name': 'Product example', ...}, ...]

         >>> get_product_list("", "invalid_client_id", "api-key")
         Client Error: Unauthorized for url
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Gets a list of offerids (articles) of all products in a store on Ozon 
    via the paginated API

    Args:
        client_id (str): unique client number for identification in the system.
        seller_token (str): api-key (token) of the seller.

    Returns:
        list: List of offerid string values ​​for all found products.

    Examples:
        >>> ids = get_offer_ids("client_id", "seller_token")
        >>> isinstance(ids, list)
        True
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Generates and sends a POST request to the Ozon price import method 
    with the passed list of prices

    Args:
        prices (list): a list of dictionaries with price objects.
        
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки"""
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Downloads an archive with balances from a fixed URL, 
    extracts an Excel file from it, 
    reads data starting from the 18th line and returns a list of records

    Args:
        no arguments.
        
    Returns:
        list: List of dictionaries with data from Excel.

    Examples:
        >>> remnants = downloadstock()
        >>> isinstance(remnants, list)
        True

        >>> downloadstock()
        requests.exceptions.ConnectionError: ...
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """It goes through the list of remaining items (watchremnants) 
    and for those items whose code is present in offerids, 
    it forms a dictionary with price information (corresponds to the Ozon format).
    
    Args:
        watch_remnants (list)
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Converts a string representation of a price to a numeric value.
    
    The function accepts a phrase with a price and saves 
    the fragment containing only the numeric portion without separators or symbols.

    Args:
        price (str): A string representation of the price, 
        stripped of all non-numeric characters.
        
    Returns:
         str: The numeric representation of the price, 
         stripped of all non-numeric characters.
         
    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Generates a sequence of sublists of the original list, each of length at most n.

    Args:
        lst (list): Initial list of elements.
        n (int): The size of each part; must be a positive integer.

    Returns:
        list: A generator that returns sublists (each a list of at most n elements).

    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]] 

        >>> list(divide([1, 2, 3], 0))
        ValueError: n must be a positive integer
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
