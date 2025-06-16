DOCKER_NAME = "weather-producer"
BUILD_CONTEXT = "weather_producer/"

build:
	docker build -t ${DOCKER_NAME} -f weather_producer/Dockerfile ${BUILD_CONTEXT}

run:
	docker run --env-file .env ${DOCKER_NAME}

build-push-az:
	docker build -t weatherarcz-dcd4cyfghhf0abbt.azurecr.io/weather_producer:latest -f weather_producer/Dockerfile ${BUILD_CONTEXT}
	docker push weatherarcz-dcd4cyfghhf0abbt.azurecr.io/weather_producer:latest
