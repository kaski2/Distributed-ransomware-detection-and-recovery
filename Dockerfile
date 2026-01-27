FROM python:3.12-slim

WORKDIR /app/

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/


# TODO CHANGE 'THE_PORT_YOU_WANT_TO_EXPOSE_HERE' TO THE ACTUAL PORT NUMBER
EXPOSE 1337

# TODO CHANGE 'IDK' TO THE ACTUAL FILE NAME
CMD ["python", "IDK"]