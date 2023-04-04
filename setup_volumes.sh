#!/bin/bash
cp -r dags/ /mnt/dockeracivolumes/dags/
cp -r . /mnt/dockeracivolumes/sources/
cp analytics-config/postgresql.conf /mnt/dockeracivolumes/analytics-config/
cp data/northwind.sql /mnt/dockeracivolumes/init-db-data/
