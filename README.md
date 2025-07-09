# Celery flask docker

This project uses Celery for distributed task queuing with Redis (Valkey) as a message broker and result backend. It includes a web service, a Celery worker, and Flower for monitoring.

## Services

*   **Valkey:** A high-performance, open-source advanced key-value store. Used as the message broker and result backend for Celery.
*   **Celery Worker:** Processes background tasks defined in `app/tasks.py`.
*   **Flower:** A web-based tool for monitoring and administrating Celery clusters.
*   **Web:** The main web application (details to be filled in from `web/app.py`).

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

*   Docker
*   Docker Compose

### Installation

1.  **Clone the repository:**
    ```bash
    git clone git@github.com:RANUX/celery-flask-docker.git
    cd celery-flask-docker
    ```

2.  **Build and run the services:**
    ```bash
    docker-compose up --build
    ```

    This command will:
    *   Build the `celery` and `web` service images based on their respective Dockerfiles.
    *   Start all the services defined in `docker-compose.yml` (Valkey, Celery, Flower, Web).

3. **Run the services:**
    ```bash
    docker-compose up
    ```

## Usage

Once all services are up and running:

*   **Web Application:** Access the web application at `http://localhost:8000` (assuming `web/app.py` serves content on this port).
*   **Flower Monitoring:** Access the Flower dashboard at `http://localhost:5555`. Here you can monitor the status of your Celery tasks, workers, and more.

## Project Structure

*   `app/`: Contains the Celery application and task definitions (`tasks.py`).
*   `web/`: Contains the main web application code (e.g., `app.py`).
*   `docker-compose.yml`: Defines the services, networks, and volumes for the multi-container Docker application.

## Celery Tasks

The `app/tasks.py` file defines several example Celery tasks:

*   `add(x, y)`: Returns the sum of `x` and `y`.
*   `sleep(seconds)`: Pauses execution for the specified number of seconds.
*   `echo(msg, timestamp=False)`: Returns the message, optionally prepended with a timestamp.
*   `error(msg)`: Raises an exception with the given message.

You can call these tasks from your web application (or elsewhere) to be executed by the Celery worker.

## Environment Variables

The following environment variables are used for Celery configuration:

*   `CELERY_BROKER_URL`: URL for the Celery message broker (e.g., `redis://valkey:6379/0`).
*   `CELERY_RESULT_BACKEND`: URL for the Celery result backend (e.g., `redis://valkey:6379/0`).

These are configured within `docker-compose.yml` to point to the `valkey` service.

## License
Use the `MIT` license.
