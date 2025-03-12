import sys
import gi
gi.require_version('Gst', '1.0')
from gi.repository import GObject, Gst, GLib

import os
from frameCallback import probe_callback  # Import your callback function here

# Initialize GStreamer
Gst.init(None)

def main():
    # Make sure you correct any typos in your tracker config file name if needed!
    pipeline_str = (
        'v4l2src device="/dev/video0" '
        '! capsfilter caps="image/jpeg, width=1920, height=1080, framerate=30/1" '
        '! jpegdec '
        '! videoconvert '
        '! nvvideoconvert '
        '! capsfilter caps="video/x-raw(memory:NVMM), format=RGBA, width=1920, height=1080, framerate=30/1" '
        '! mux.sink_0 '

        'nvstreammux name="mux" batch-size=1 width=1920 height=1080 '
        'batched-push-timeout=4000000 live-source=1 num-surfaces-per-frame=1 '
        'sync-inputs=0 max-latency=0 '

        '! nvinfer name="primary-inference" '
        '   config-file-path="./config/YOLOV8S.yml" '

        '! nvtracker tracker-width=640 tracker-height=384 gpu-id=0 '
        '   ll-lib-file="/opt/nvidia/deepstream/deepstream/lib/libnvds_nvmultiobjecttracker.so" '
        '   ll-config-file="./config/config_tracker_NvDCF_perf.yml" '  # <-- Adjust if needed

        '! nvdsanalytics name="analytics" '
        '   config-file="./config/analytics.txt" '

        '! nvvideoconvert '
        '! nvdsosd '
        '! nveglglessink'
    )

    # Parse and create the pipeline
    pipeline = Gst.parse_launch(pipeline_str)
    if not pipeline:
        print("Failed to create pipeline from the string.")
        return

    # --------------------------------------------------------------------
    # Attach the probe callback to the nvinfer (primary-inference) element
    # If you really want to probe after analytics, change the name to "analytics".
    # --------------------------------------------------------------------
    nvinfer_elem = pipeline.get_by_name("analytics")
    if not nvinfer_elem:
        print("Failed to get nvinfer element by name 'primary-inference'")
        return

    # Typically we attach to the src pad of nvinfer
    nvinfer_src_pad = nvinfer_elem.get_static_pad("src")
    if not nvinfer_src_pad:
        print("Failed to get the nvinfer src pad")
    else:
        nvinfer_src_pad.add_probe(Gst.PadProbeType.BUFFER, probe_callback)

    # Create a GLib Main Loop to handle GStreamer messages
    loop = GLib.MainLoop()

    # Add a bus watch to handle errors and other messages
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    # Start the pipeline
    print("Starting pipeline...")
    pipeline.set_state(Gst.State.PLAYING)

    try:
        loop.run()
    except KeyboardInterrupt:
        pass

    # Clean up
    print("Exiting pipeline...")
    pipeline.set_state(Gst.State.NULL)

def bus_call(bus, message, loop):
    msg_type = message.type
    if msg_type == Gst.MessageType.EOS:
        print("End-of-Stream")
        loop.quit()
    elif msg_type == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        print(f"Error: {err}, {debug}")
        loop.quit()
    return True

if __name__ == "__main__":
    sys.exit(main())
