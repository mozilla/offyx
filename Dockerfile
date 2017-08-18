FROM python:3.6
ENV PORT=8000
EXPOSE $PORT
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY offyx.py .
COPY ua.py .
COPY version.json .
CMD FLASK_APP=offyx.py flask run --host 0.0.0.0 --port $PORT
