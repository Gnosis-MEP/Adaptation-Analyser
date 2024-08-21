# Adaptation Analyser
Analyse in the MAPE-K architecture, checks if any QoS policy rules are being broken (e.g.: a worker is overloaded), and if so, it publishes a requests for a specific change on the system planning.

# Events Listened
 - [QUERY_CREATED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#QUERY_CREATED)
 - [SERVICE_SLR_PROFILES_RANKED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#SERVICE_SLR_PROFILES_RANKED)
 - [SERVICE_WORKERS_STREAM_MONITORED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#SERVICE_WORKERS_STREAM_MONITORED)
 - [SERVICE_WORKER_ANNOUNCED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#SERVICE_WORKER_ANNOUNCED)
 - [SCHEDULING_PLAN_EXECUTED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#SCHEDULING_PLAN_EXECUTED)

# Events Published
 - [NEW_QUERY_SCHEDULING_PLAN_REQUESTED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#NEW_QUERY_SCHEDULING_PLAN_REQUESTED)
 - [SERVICE_WORKER_SLR_PROFILE_CHANGE_PLAN_REQUESTED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#SERVICE_WORKER_SLR_PROFILE_CHANGE_PLAN_REQUESTED)
 - [SERVICE_WORKER_OVERLOADED_PLAN_REQUESTED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#SERVICE_WORKER_OVERLOADED_PLAN_REQUESTED)
 - [SERVICE_WORKER_BEST_IDLE_REQUESTED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#SERVICE_WORKER_BEST_IDLE_REQUESTED)
 - [UNNECESSARY_LOAD_SHEDDING_REQUESTED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#UNNECESSARY_LOAD_SHEDDING_REQUESTED)


# Installation

## Configure .env
Copy the `example.env` file to `.env`, and inside it replace the variables with the values you need.

## Installing Dependencies

### Using pipenv
Run `$ pipenv shell` to create a python virtualenv and load the .env into the environment variables in the shell.

Then run: `$ pipenv install` to install all packages, or `$ pipenv install -d` to also install the packages that help during development, eg: ipython.
This runs the installation using **pip** under the hood, but also handle the cross dependency issues between packages and checks the packages MD5s for security mesure.


### Using pip
To install from the `requirements.txt` file, run the following command:
```
$ pip install -r requirements.txt
```

# Running
Enter project python environment (virtualenv or conda environment)

**ps**: It's required to have the .env variables loaded into the shell so that the project can run properly. An easy way of doing this is using `pipenv shell` to start the python environment with the `.env` file loaded or using the `source load_env.sh` command inside your preferable python environment (eg: conda).

Then, run the service with:
```
$ ./adaptation_analyser/run.py
```

# Testing
Run the script `run_tests.sh`, it will run all tests defined in the **tests** directory.

Also, there's a python script at `./adaptation_analyser/send_msgs_test.py` to do some simple manual testing, by sending msgs to the service stream key.


# Docker
## Build
Build the docker image using: `docker-compose build`

**ps**: It's required to have the .env variables loaded into the shell so that the container can build properly. An easy way of doing this is using `pipenv shell` to start the python environment with the `.env` file loaded or using the `source load_env.sh` command inside your preferable python environment (eg: conda).

## Run
Use `docker-compose run --rm service` to run the docker image


## Gitlab CI auto-build and tests

This is automatically enabled for this project (using the `.gitlab-ci.yml` present in this project root folder).

By default it will build the Dockerfile with every commit sent to the origin repository and tag it as 'dev'.

Afterwards, it will use this newly builty image to run the tests using the `./run_tests.sh` script.

But in order to make the automatic docker image build work, you'll need to set the `SIT_PYPI_USER` and `SIT_PYPI_PASS` variables in the Gitlab CI setting page: [Adaptation Analyser CI Setting Page](https://gitlab.insight-centre.org/sit/mps/adaptation-analyser/settings/ci_cd).

And, in order to make the automatic tests work, you should also set the rest of the environement variables required by your service, in the this projects `.gitlab-ci.yml` file, in the `variables` section. But don't add sensitive information to this file, such as passwords, this should be set through the Gitlab CI settings page, just like the `SIT_PYPI_USER`.

## Benchmark Tests
To run the benchmark tests one needs to manually start the Benchmark stage in the CI pipeline, it shoud be enabled after the tests stage is done. Only by passing the benchmark tests shoud the image be tagged with 'latest', to show that it is a stable docker image.



