FROM python:3.10-slim

# WORKDIR /weather_producer

COPY requirements.txt .
COPY generate_weather_messages.py .
RUN pip install -r requirements.txt

CMD ["python", "generate_weather_messages.py"]
