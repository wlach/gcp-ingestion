FROM python:3.7-slim AS common
WORKDIR /app

# build in separate stage to reduce image size
FROM common AS build
RUN apt-get update && apt-get install -y gcc git
COPY constraints.txt requirements.txt ./
COPY bin/include/common.sh ./bin/include/
COPY bin/build ./bin/
ENV VENV=false
RUN bin/build

FROM common
RUN apt-get update && apt-get install -y wrk
COPY --from=build /usr/local /usr/local
COPY . .
CMD python -m ingestion.sink
