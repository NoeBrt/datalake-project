FROM  nvcr.io/nvidia/deepstream-l4t:7.0-triton-multiarch


# Set environment variables
ENV LD_LIBRARY_PATH=/usr/local/cuda-12.2/lib64:$LD_LIBRARY_PATH
ENV CUDA_VER=12.2


# Update and install dependencies
RUN apt-get update && apt-get install -y \
    # Python and development tools
    python3-pip \
    python3-gi \
    python3-dev \
    python3-setuptools \
    # python3-opencv \
    build-essential \
    cmake \
    pkg-config \
    \
    # System utilities
    # Libraries
    libusb-1.0-0-dev \
    libcairo2-dev \
    libmpg123-0 \
    libavfilter7 \
    libopenmpt0 \
    libmpeg2-4 \
    libmpeg2encpp-2.1-0 \
    libx265-199 \
    libavcodec58 \
    libvpx7 \
    libavcodec58 \
    gstreamer1.0-plugins-{base,good,bad,ugly} \
    gstreamer1.0-libav\
    # GStreamer and related packages
    gstreamer1.0-tools \
    gstreamer1.0-alsa \
    gstreamer1.0-libav \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer-plugins-good1.0-dev \
    libgstreamer-plugins-bad1.0-dev \
    # Miscellaneous
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip


# Clone and build libbtbb repository
# RUN git clone https://github.com/greatscottgadgets/libbtbb.git /root/libbtbb && \
#     cd libbtbb && \
#     mkdir build && \
#     cd build &&  \
#     cmake ..  &&  \
#     make  &&  \
#     make install  &&  \
#     ldconfig





# Install Python packages
RUN pip3 install opencv-python \
pandas \
psutil \
kafka-python \
glob2

# Install specific version of pyds
COPY ./people_tracking/pip-installs pip-installs
RUN pip3 install ./pip-installs/* && \
rm -rf pip-installs





# Set working directory
WORKDIR /app

# Copy the contents of the current directory to the /app directory in the container
COPY ./people_tracking /app
RUN make -C /app/nvdsinfer_custom_impl_Yolo clean && make -C /app/nvdsinfer_custom_impl_Yolo


# Create user 'floware' and set permissions
RUN useradd -m floware && \
echo "floware:password" | chpasswd && \
usermod -aG sudo floware && \
chown -R floware:floware /app

