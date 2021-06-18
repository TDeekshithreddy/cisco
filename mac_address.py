import sys
import re
import requests
import argparse
import logging
logging.basicConfig(stream=sys.stdout,
                    format='%(message)s',
                    level=logging.INFO,
                    datefmt='%d-%b-%y %H:%M:%s')

api_url = 'https://api.macaddress.io/v1?apiKey=at_pmpc02z05jyIP0EhjvfNADQDuNkMC&output=json&search={mac_addresss}'
VENDOR_DETALS = 'vendorDetails'
BLOCK_DETAILS = 'blockDetails'
MAC_ADDRESS_DETAIL = 'macAddressDetails'


class APiRequestHandler:
    @staticmethod
    def retry_loop(url, retry_count):
        session = requests.Session()
        for _ in range(retry_count):
            try:
                retry_response = session.get(url, verify=False, timeout=60)
            except requests.exceptions.Timeout:
                continue
            except requests.exceptions.ConnectionError as err:
                logging.error('The connection was refused while making API request for API: %s, Error: %s', url, str(err))
                return None
            except requests.exceptions.RequestException as err:
                logging.error('Request exception while making API request for API: %s, Error: %s', url, str(err))
                return None
            else:
                return retry_response

        logging.error('Retry failed %s times', retry_count)
        return None

    @staticmethod
    def make_api_request(url):
        try:
            session = requests.Session()
            response = session.get(url, verify=False, timeout=60)
        except requests.exceptions.Timeout:
            logging.info('API request for %s timed out. Retrying....', url)
            return APiRequestHandler.retry_loop(url, 3)
        except requests.exceptions.ConnectionError as err:
            logging.error('The connection was refused while making API request for API: %s, Error: %s', url, str(err))
            return None
        except requests.exceptions.RequestException as err:
            logging.error('Request exception while making API request for API: %s, Error: %s', url, str(err))
            return None
        else:
            if response.status_code != 200:
                logging.error('The API request for %s failed with error: %s', url, response.status_code)
                return None
            return response


class MACRequestHandler:
    def __init__(self, mac_address):
        self.mac_address = mac_address
        self.url = api_url.format(mac_addresss=mac_address)

    def get_mac_details(self, param):
        mac_response = APiRequestHandler.make_api_request(self.url)
        if not mac_response:
            return None
        mac_add_details = mac_response.json()
        return mac_add_details[param]


class Args:
    @staticmethod
    def parse_arguments():
        """ Parse arguments provided by spark-submit commend"""
        parser = argparse.ArgumentParser()
        parser.add_argument("--mac_address", required=True)
        return parser.parse_args()

    @staticmethod
    def validate_args(args):
        for arg, val in args._get_kwargs():
            if arg == 'mac_address':
                if not re.match("[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", val.lower()):
                    raise ValueError('MAC address format is incorrect. Given mac address is: ' + val)


def main():
    """ Main function excecuted by spark-submit command"""
    try:
        args = Args.parse_arguments()
        Args.validate_args(args)
        mac_handler = MACRequestHandler(args.mac_address)
        mac_add_detail = mac_handler.get_mac_details(VENDOR_DETALS)

        info_string = 'Vendor Details for Given MAC address: ' + args.mac_address
        logging.info(info_string)
        logging.info("=" * len(info_string))
        logging.info('%s', mac_add_detail['companyName'])
    except ValueError as err:
        logging.error(str(err))


if __name__ == "__main__":
    main()
