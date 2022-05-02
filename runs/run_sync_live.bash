#!/bin/bash
cd /home/sync-database-vfrlive/debezium_syncdb
python catalog_product_entity_main.py &
python catalog_product_entity_int.py &
python catalog_product_entity_varchar.py
