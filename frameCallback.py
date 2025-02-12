
import sys
import gi
gi.require_version('Gst', '1.0')
from gi.repository import GObject, Gst, GLib

def probe_callback(pad, info):
    buffer = info.get_buffer()
    if not buffer:
        print("Unable to get GstBuffer")
        return Gst.PadProbeReturn.OK

    print("Buffer received at probe callback")
    # Process the buffer (e.g., extract metadata or perform some analysis)

    return Gst.PadProbeReturn.OK
