FROM python:3.10-slim

WORKDIR /app

# COPY requirements.txt /app/requirements.txt
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt


CMD ["python", "utils/insert_data.py"]
