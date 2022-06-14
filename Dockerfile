FROM python:3.9.9-slim-buster AS builder
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
# CMD [ "python, "./bild.py" ]
