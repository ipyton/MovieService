import os
import traceback

import ffmpeg
def encodeHls(input_path, output_path, input_source, output_source):
    try:
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        meta = ffmpeg.probe(input_path)
        for stream in meta["streams"]:
            if stream['codec_name'] == 'h264':
                ffmpeg.input(input_path).output(os.path.join(output_path, "index.m3u8"),
                                                                   format="hls",
                                                                   hls_time=10,
                                                                   hls_list_size=0,
                                                                   hls_segment_filename=os.path.join(output_path,
                                                                                                     "segment_%03d.ts"),
                                                                   vcodec='copy',
                                                                   acodec='copy'
                                                                   ).run()
                return True
            else:
                ffmpeg.input(input_path).output(os.path.join(output_path, "index.m3u8"),
                                                                   format="hls",
                                                                   hls_time=10,
                                                                   hls_list_size=0,
                                                                   hls_segment_filename=os.path.join(output_path,
                                                                                                     "segment_%03d.ts"),
                                                                   vcodec='libx264',
                                                                   acodec='copy'
                                                                   ).run()
                return True
    except Exception as e:
        traceback.print_stack()
        return False

    return True