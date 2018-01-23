import logging
from pymongo import MongoClient
from web3 import Web3, HTTPProvider


logging.basicConfig(level=logging.INFO)

client = MongoClient()
db = client.blockfront
web3 = Web3(HTTPProvider('http://node.blockfront.io:8545'))

for n in range(4000000, 4000100):
    logging.info("Fetching block %d", n)
    block = web3.eth.getBlock(n, True)
    for tx in block['transactions']:
        logging.info("Denormalizing %s", tx.hash)
        for address in (tx['from'], tx['to']):
            logging.info("Updating address %s with tx %s", address, tx['hash'])
            logging.info("%d", tx['value'])
            db.addresses.update_one(
                    { "hash": address },
                    {
                        "$push": {
                            "txs": {
                                "hash": tx['hash'],
                                "block": block['number'],
                                "timestamp": block['timestamp'],
                                "from": tx['from'],
                                "to": tx['to'],
                                "value": str(tx['value']) # XXX Too big !
                            }
                        }
                    },
                    upsert=True
            )
