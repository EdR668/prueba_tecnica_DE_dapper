.PHONY: reset-airflow init-airflow up-airflow down-airflow start build-airflow install

down-airflow:
	docker-compose down --volumes

# Limpieza total opcional (comentado por seguridad)
# reset-airflow: down-airflow
# 	sudo chown -R $$(id -u):$$(id -g) logs dags plugins || true
# 	rm -rf logs/* dags/* plugins/*
# 	mkdir -p logs dags plugins
# 	chmod 777 logs dags plugins

init-airflow:
	docker-compose run --rm webserver airflow db init
	docker-compose run --rm webserver \
	  airflow users create \
	    --username admin --password admin \
	    --firstname Admin --lastname User \
	    --role Admin --email admin@example.com

up-airflow:
	docker-compose up -d

build-airflow:
	docker-compose up --build -d

start: init-airflow up-airflow

install: down-airflow build-airflow init-airflow up-airflow
	@echo "✅ Airflow instalado y listo para usar."
