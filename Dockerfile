FROM registry.insight-centre.org/sit/mps/docker-images/base-services:latest

## install only the service requirements
ADD ./Pipfile /service/Pipfile
ADD ./requirements.txt /service/requirements.txt
ADD ./setup.py /service/setup.py
RUN mkdir -p /service/adaptation_analyser/ && \
    touch /service/adaptation_analyser/__init__.py
WORKDIR /service
# RUN rm -f Pipfile.lock && pipenv lock -vvv && pipenv --rm && \
#     pipenv install --system  && \
#     rm -rf /tmp/pip* /root/.cache
RUN pip install -r requirements.txt && \
    rm -rf /tmp/pip* /root/.cache

## add all the rest of the code and install the actual package
## this should keep the cached layer above if no change to the pipfile or setup.py was done.
ADD . /service
RUN pip install -e . && \
    rm -rf /tmp/pip* /root/.cache
