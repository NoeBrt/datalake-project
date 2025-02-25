import sys
import gi
gi.require_version('Gst', '1.0')
from gi.repository import GObject, Gst, GLib
import time
import os
import pyds
from datetime import datetime

ROTATE_FILE_TIME = 60 # seconds
FILE_NAME = "vision"
EXTENSION = "csv"
DATA_FOLDER = "data"


# Ensure the data folder exists
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

current_file_start = 0
current_output_file = None

def get_output_file(rotate_time, file_name, extension):
    """
    Returns the current output file path. If the time since the file was created
    exceeds rotate_time, a new file with a timestamp in its name is created.
    """
    global current_file_start, current_output_file
    now = time.time()
    if current_output_file is None or (now - current_file_start) >= rotate_time:
        # Generate a new file name with the current timestamp.
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        current_output_file = os.path.join(DATA_FOLDER, f"{file_name}_{timestamp}.{extension}")
        current_file_start = now
        print("Switched to new file:", current_output_file)
        with open(current_output_file, "w") as file:
                    file.write("frame_num,object_id,class_id,confidence,top,left,width,height,timestamp\n")
    return current_output_file


def probe_callback(pad, info):
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        print("Unable to get GstBuffer")
        return Gst.PadProbeReturn.OK
    process_frame(gst_buffer)
    return Gst.PadProbeReturn.OK

def process_frame(gst_buffer):
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    if not batch_meta:
        print("Unable to get batch metadata")
        return Gst.PadProbeReturn.OK

    # Iterate over all frames in the batch.
    l_frame = batch_meta.frame_meta_list
    while l_frame:
        # Cast frame meta to NvDsFrameMeta
        frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        l_frame = l_frame.next  # move to next frame meta
        timestamp= datetime.now().strftime('%Y%m%d-%H%M%S')

        # Iterate over all objects in the frame.
        l_obj = frame_meta.obj_meta_list
        while l_obj:
            obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            rect_params = obj_meta.rect_params
            class_id = int(obj_meta.class_id)
            object_id = obj_meta.object_id

            # Check for the unsigned 64-bit max value (invalid object_id)
            if object_id == 0xFFFFFFFFFFFFFFFF:
                print("Skipping object with invalid id")
                l_obj = l_obj.next
                continue
            # Get the proper output file path (rotated if needed)
            output_file = get_output_file(ROTATE_FILE_TIME, FILE_NAME, EXTENSION)
            # Open in append mode so that data is added to the file.
            print("Writing to file:", object_id)
            with open(output_file, "a") as result_file:
                result_file.write(
                    f"{frame_meta.frame_num},{object_id},{class_id},{obj_meta.confidence},"
                    f"{rect_params.top},{rect_params.left},{rect_params.width},{rect_params.height},"
                    f"{timestamp}\n"
                )

            l_obj = l_obj.next  # move to next object meta
