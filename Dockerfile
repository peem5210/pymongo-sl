FROM python:3.9
COPY . .
RUN pip3 install -r requirements.txt
CMD ["python3", "rcache_profiling.py", "-s 100000", "-r 150", "-a", "--connection", "SGP_1,ORE_1", "-o docker"]
