# Thesis: Geospatial data integration

## Setup
1. Download the google service account key with BigQuery permissions
2. Store the key in `.secrets/target_bigquery_secret.json`

## Docker
1. To start Docker compose simply run `docker-compose up`
2. You can log into the meltano container by running `docker exec -it thesis-meltano bash`
3. You can test a pipeline by running `meltano elt tap-osm target-bigquery`