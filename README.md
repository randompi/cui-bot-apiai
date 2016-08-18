CUI POC Api.ai Bot
=============

## Overview
The Conversational User Interface (CUI) proof-of-concept bot integrates with [api.ai](http://api.ai) on the back-end for intent matching, entity extraction, and slot filling in order to fulfill different conversational dialog scenarios in the medical domain.

## Usage

### Environment Setup
It's recommended to download Anaconda 2.7 Python environment from: https://www.continuum.io/downloads

Then setup a matplotlib supporting environment (to support bot's plotting capabilities):

    conda create -n matplotenv matplotlib
    source activate matplotenv

### Run the bot locally
Install dependencies:

	pip install -r requirements.txt

Run the bot exectuable setting the slack token from https://robotsandpencils.slack.com/services/B152HETM5:

	export SLACK_TOKEN=<YOUR SLACK TOKEN>; python ./bot/app.py

Things are looking good if the console prints something like:

	Connected <your bot name> to <your slack team> team at https://<your slack team>.slack.com.

If you want change the logging level, prepend `export LOG_LEVEL=<your level>; ` to the `python ./bot/app.py` command.

### Run locally in Docker
	docker build -t starter-python-bot .
	docker run --rm -it -e SLACK_TOKEN=<YOUR SLACK API TOKEN> starter-python-bot

### Run in BeepBoop
Project page: https://beepboophq.com/0_o/my-projects/84ea846d69d042b3970717f50b7115fd

changes pushed to the remote master branch will automatically deploy.

### Run Unit Tests
From the root of the project directory:

	python -m unittest discover -s tests -p "*_test.py"

## Code Organization
The entry point for the bot is [app.py](bot/app.py) which is responsible for collecting environment variables like log level and slack token, and creating/starting a SlackBot instance from [slack_bot.py](bot/slack_bot.py).

The `slack_bot.py` module implements an interface that is needed to run a multi-team bot using the Beep Boop Resource API client, by implementing an interface that includes `start()` and `stop()` methods and a function that spawns new instances of your bot: `spawn_bot`.  It is the main run loop of your bot instance that will listen to a particular Slack team's RTM events, and dispatch them to the `event_handler`.

If you want to add or change an event that the bot responds (e.g. when the bot is mentioned, when the bot joins a channel, when a user types a message, etc.), you can modify the `_handle_by_type` method in `event_handler.py`.

If you want to change the responses, then you can modify the `messenger.py` class, and make the corresponding invocation in `event_handler.py`.

The `slack_clients.py` module provides a facade of two different Slack API clients which can be enriched to access data from Slack that is needed by your Bot:

1. [slackclient](https://github.com/slackhq/python-slackclient) - Realtime Messaging (RTM) API to Slack via a websocket connection.
2. [slacker](https://github.com/os/slacker) - Web API to Slack via RESTful methods.



