DOCKER_NAME = "weather-producer"
BUILD_CONTEXT = "weather_producer/"

build:
	docker build -t ${DOCKER_NAME} -f weather_producer/Dockerfile ${BUILD_CONTEXT}

run:
	docker run --env-file .env weather_producer