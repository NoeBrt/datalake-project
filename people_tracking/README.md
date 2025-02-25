<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>


<!-- PROJECT LOGO -->
<div align="center">

# FLWR Vision Nano
  
<br />

<img src="https://github.com/Floware-FR/flwr-vision-nano/assets/94910317/02f9ff42-eb4a-4858-a23f-dd93c8d04c33" alt="Logo" width="500" height="500">

<div>
  <a href="https://github.com/Floware-FR/flwr-vision-nano/actions/workflows/build_and_push.yml"><img src="https://github.com/Floware-FR/flwr-vision-nano/actions/workflows/build_and_push.yml/badge.svg" alt="flwr-vision-nano CI"></a>
  <a href="https://hub.docker.com/r/noebrt/flwr-nano-app"><img src="https://img.shields.io/docker/pulls/noebrt/flwr-nano-app?logo=docker" alt="Docker Pulls"></a>
    <br>
</div>
<br>


FLWR Vision Nano is a computer vision application designed to run on NVIDIA Jetson Nano. It leverages GStreamer for video processing and DeepStream SDK for inference, tracking, and analytics. This project supports various inference models and provides mechanisms to convert and store results.

FLWR Vision Nano also manages Bluetooth data with Ubertooth when a Bluetooth antenna is plugged into the Jetson.
</div>

## Table of Contents

- [FLWR Vision Nano](#flwr-vision-nano)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [Without Docker](#without-docker)
    - [With docker](#with-docker)
  - [Configuration](#configuration)
  - [Services](#services)
  - [Usage](#usage)
    - [Usage with SystemD Services](#usage-with-systemd-services)
    - [Usage on Docker](#usage-on-docker)
  - [Focus on Important Scripts](#focus-on-important-scripts)
    - [FLWR-Vision.py](#flwr-visionpy)
    - [Analytics.py](#analyticspy)
    - [FLWR-UbertoothService.py](#flwr-ubertoothservicepy)
    - [FLWR-UbertoothLogsToParquet.py](#flwr-ubertoothlogstoparquetpy)
    - [FLWR-SyncFoldersDaemon.py](#flwr-syncfoldersdaemonpy)
  - [Project Structure](#project-structure)
  - [Contributors](#contributors)
  - [Other Useful Documents](#other-useful-documents)

## Introduction

FLWR Vision Nano provides a robust and scalable solution for real-time video analytics using NVIDIA's DeepStream SDK. It supports multiple inference models for primary detection, secondary detection, and recognition. Its purpose is to analyze traffic flow.

## Features

- Real-time video processing with GStreamer
- Primary and secondary inference using DeepStream SDK
- Customizable analytics and tracking via a custom configuration file
- Processing Bluetooth data using Ubertooth
- CSV to Parquet conversion for results storage
- Sending data to a remote server

## Prerequisites

Before you begin, ensure you have met the following requirements:

- NVIDIA Jetson Nano Developer Kit
- NVIDIA JetPack SDK (includes L4T, CUDA, cuDNN, and TensorRT)
- DeepStream SDK 6.0
- Python 3.6 or later
- GStreamer 1.14 or later
- Ubertooth & libbtbb

## Installation

### Without Docker

1. **Clone the repository**:
    ```bash
    git clone https://github.com/your-username/flwr-vision-nano.git FLWR-Vision
    cd FLWR-Vision
    ```

2. **Update the submodules**:
    ```bash
    git submodule update --init --recursive
    ```

3. **Install dependencies**:
    ```bash
    sudo apt-get update
    sudo apt-get install -y python3-pip
    pip3 install -r requirements.txt
    ```

4. **Setup DeepStream SDK**:
    Follow the official [DeepStream SDK documentation](https://docs.nvidia.com/metropolis/deepstream/dev-guide/index.html) to set up the SDK on your Jetson Nano.

5. **Setup The Jetson Nano Entirely**:
    Run `setup_nano.sh` from [Setup Jetson](https://github.com/Floware-FR/setup_flwr_jetson.git).

### With docker

pull docker image from noebrt/flwr-nano-app


```bash
xhost +
export DISPLAY=:0
sudo docker run -d --runtime=nvidia --name "flwr_container" --gpus all --privileged -it \
    -v /tmp/.X11-unix:/tmp/.X11-unix -e DISPLAY=$DISPLAY \
    --device=/dev/ttyUSB0 \
    --device=/dev/bus/usb \
    $(for device in /dev/video*; do echo --device $device; done) \
    --env LD_LIBRARY_PATH=/usr/local/cuda-10.2/lib64:$LD_LIBRARY_PATH \
    --ipc=host \
    --ulimit memlock=-1 --ulimit stack=67108864 \
    --network host --rm noebrt/flwr-nano-app /bin/bash
```

1. `docker run`: Runs a Docker container.

2. `-d`: Runs the container in detached mode (in the background).

3. `--runtime=nvidia`: Specifies the NVIDIA runtime to use for GPU support.

4. `--name $CONTAINER_NAME`: Assigns a name to the container, replacing `$CONTAINER_NAME` with the actual container name.

5. `--gpus all`: Allocates all available GPUs to the container.

6. `--privileged`: Grants the container extended privileges, allowing access to devices and certain host features.

7. `-it`: Combines `-i` (interactive mode) and `-t` (allocates a pseudo-TTY), allowing interaction with the container.

8. `-v /tmp/.X11-unix:/tmp/.X11-unix`: Mounts the host's X11 socket into the container, enabling GUI applications to display on the host's X server.

9. `-e DISPLAY=$DISPLAY`: Sets the `DISPLAY` environment variable in the container to match the host's display settings, allowing GUI applications to display correctly.

10. `--device=/dev/ttyUSB0`: Grants the container access to the `/dev/ttyUSB0` device, typically used for serial communication.

11. `--device=/dev/bus/usb`: Grants the container access to USB devices connected to the host.

12. `$(for device in /dev/video*; do echo --device $device; done)`: Adds all video devices (e.g., webcams) from the host to the container, enabling access to them.

13. `--env LD_LIBRARY_PATH=/usr/local/cuda-10.2/lib64:$LD_LIBRARY_PATH`: Sets the `LD_LIBRARY_PATH` environment variable in the container, including CUDA libraries for GPU support.

14. `--ipc=host`: Shares the host's IPC namespace with the container, improving performance for certain types of applications.

15. `--ulimit memlock=-1`: Sets the memory lock limit to unlimited, allowing the container to lock as much memory as needed.

16. `--ulimit stack=67108864`: Sets the stack size limit to 64 MB.

17. `--network host`: Uses the host's network stack, allowing the container to access network resources as if it were running on the host.

18. `--rm`: Automatically removes the container when it exits, keeping the system clean.

19. `noebrt/flwr-nano-app`: Specifies the Docker image to use for the container.

20. `/bin/bash`: The command to run inside the container, in this case, starting a bash shell.



## Configuration

The application uses configuration files for setting various parameters.

- **FLWR-Vision-config.txt**: Main configuration file.
- **cfg/analytics.txt**: Configuration for analytics settings.
- **cfg/dstest2_pgie_config.txt**: Configuration for the primary inference model.
- **cfg/config_infer_secondary_lpdnet.txt**: Configuration for the secondary inference model (e.g., license plate detection).
- **cfg/config_infer_secondary_lprnet.txt**: Configuration for the recognition model (e.g., license plate recognition).

Example configuration snippet from `FLWR-Vision-config.txt`:

```ini
[app]
DISPLAY=1
MODEL-PATH=path/to/primary/model/config
SECONDARY-DETECTION-MODEL=1 # 1 | 0 to integrate it in the pipeline
SECONDARY-DETECTION-MODEL-PATH=path/to/secondary/model/config
SECONDARY-RECOGNITION-MODEL=1 # 1 | 0 to integrate it in the pipeline
SECONDARY-RECOGNITION-MODEL-PATH=path/to/recognition/model/config
ANALYTICS-CONFIG-PATH=cfg/analytics.txt
RESULTS-STORAGE=1
PARQUET-CONVERT-INTERVAL=10
TRACKER-CONFIG-PATH=cfg/dstest2_tracker_config.txt
RELAY_CONTROL=0
```

## Services

FLWR Vision uses Systemd services to run and manage its usage:

- `flwr_vision.service`: Manages `FLWR-Vision.py`
- `flwr_ubertoothservice.service`: Manages `FLWR-UbertoothService.py`
- `flwr_ubertoothlogstoparquet.service`: Manages `FLWR-UbertoothLogsToParquet.py`
- `flwr_syncfolderdaemon.service`: Manages `FLWR-SyncFoldersDaemon.py`
- `flwr_powerdaemon.service`: Manages `FLWR-PowerDaemon.py`
- `flwr_diskusagecontroldaemon.service`: Manages `FLWR-DiskUsageControlDaemon.py`

## Usage

### Usage with SystemD Services
To set up and run the services, execute:

```bash
cd FLWR-Vision
bash setup-services.sh
```

To run only the computer vision part, use:

```bash
cd FLWR-Vision
python3 FLWR-Vision.py
```

### Usage on Docker

to run FLWR-Vision on docker.
 
```bash
OPENBLAS_CORETYPE=ARMV8 /usr/bin/python3 FLWR-Vision.py
```

## Focus on Important Scripts

### FLWR-Vision.py

Manages the DeepStream computer vision service.

### Analytics.py

Handles custom analytics used for FLWR-Vision.py buffer prode function.

### FLWR-UbertoothService.py

Runs `ubertooth-rx` and saves the output in `.log` files at 10-minute intervals.

### FLWR-UbertoothLogsToParquet.py

Converts all `.log` files from `bluetooth` to `.parquet`.

### FLWR-SyncFoldersDaemon.py

Uses AirDrop to send files to the server, excluding files that are currently being written to.

## Project Structure

```
.
├── analytics.py
├── bluetooth
├── cfg
│   └── Model Config text file
├── common
│   ├── bus_call.py
│   ├── FPS.py
│   ├── __init__.py
│   ├── is_aarch_64.py
│   ├── __pycache__
│   │   ├── bus_call.cpython-310.pyc
│   │   ├── FPS.cpython-310.pyc
│   │   ├── __init__.cpython-310.pyc
│   │   └── is_aarch_64.cpython-310.pyc
│   └── utils.py
├── Dockerfile
├── Draw-roi.py
├── easyroi
│   ├── easyRoi.py
│   ├── __init__.py
│   └── utils.py
├── FLWR-CheckTeamViewer.py
├── FLWR-DiskUsageControlDaemon.py
├── FLWR-PowerDaemon.py
├── FLWR-SyncFoldersDaemon-Config.json
├── FLWR-SyncFoldersDaemon.py
├── FLWR-TeamViewerDaemon.py
├── FLWR-UbertoothLogsToParquet.py
├── FLWR-UbertoothService.py
├── FLWR-Vision-config.txt
├── FLWR-Vision.py
├── ipaddr
│   ├── floware-power-demon.py
│   └── ipaddr.txt
├── libbtbb
├── models
│   └── Models Files
├── nvdsinfer_custom_impl_Yolo
├── nvinfer_custom_lpr_parser
├── pip-installs
│   └── pyds-1.1.1-py3-none-linux_aarch64.whl
├── README.md
├── relay.py
├── requirements.txt
├── results
├── server_keys
│   └── Serveur Tokens
├── services
│   ├── flwr_diskusagecontroldaemon.service
│   ├── flwr_diskusagecontroldaemon.sh
│   ├── flwr_powerdaemon.service
│   ├── flwr_powerdaemon.sh
│   ├── flwr_syncfolderdaemon.service
│   ├── flwr_syncfolderdaemon.sh
│   ├── flwr_ubertoothlogstoparquet.service
│   ├── flwr_ubertoothlogstoparquet.sh
│   ├── flwr_ubertoothservice.service
│   ├── flwr_ubertoothservice.sh
│   ├── flwr_vision.service
│   └── flwr_vision.sh
├── setup-services.sh
├── snapshots
│   ├── time
│   └── class
└── ubertooth
```

## Contributors

- **Noé Breton** - Apprentice, Floware
- **Julian Gabriso** - CEO, Floware

## Other Useful Documents
[Dev Note on Jetson Nano](https://docs.google.com/document/d/1xEv8Liq0hy9Rwkw1Vnvnm1guXR5jPvZEIJONOa9s0Zc/edit?usp=drive_link)

---
