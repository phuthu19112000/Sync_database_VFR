FROM python:3.7.11-slim-buster
RUN apt-get update && apt-get install -y procps
WORKDIR /home/sync-database-vfrlive
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
RUN chmod +x ./runs
ENV PYTHONPATH "${PYTHONPATH}:/home/sync-database-vfrlive"
CMD [ "./runs/run_sync_live.bash" ]