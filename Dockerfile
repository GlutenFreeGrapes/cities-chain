FROM python:3.11
WORKDIR /app
COPY . .
RUN pip3 install -r requirements.txt
ENTRYPOINT ["python", "./citieschain.py"]