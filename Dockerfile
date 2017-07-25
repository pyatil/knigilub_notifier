FROM python:3.4.6-alpine3.4
COPY . /code
WORKDIR /code
RUN pip install -r requirements.txt
CMD python main.py