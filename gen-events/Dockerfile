FROM python:3
ADD gen-events.py requirements.txt /
RUN pip install -r requirements.txt
CMD [ "python", "gen-events.py" ]
