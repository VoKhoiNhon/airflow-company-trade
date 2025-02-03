build-docker:
	sh scripts/build-image.sh
deploy-docker:
	docker compose -f docker-compose.yaml --profile storage up -d
	docker compose -f docker-compose.yaml --profile workflow up -d
	docker compose -f docker-compose.yaml --profile metadata up -d
undeploy-docker:
	docker compose -f docker-compose.yaml --profile storage down | exit 0
	docker compose -f docker-compose.yaml --profile workflow down | exit 0
	docker compose -f docker-compose.yaml --profile metadata down | exit 0
test_dags:
	bash cicd/validate_dags.sh
