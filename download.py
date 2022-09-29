import argparse
import atexit
import collections
import csv
import logging.handlers
import multiprocessing as mp
import os
import random
import shutil
import sys
import traceback as tb
import urllib.request
from functools import partial

import multiprocessing_logging
from youtubesearchpython import Video

from errors import (FfmpegIncorrectDurationError, FfmpegValidationError,
                    SubprocessError)
from log import init_console_logger, init_file_logger
from utils import (HTTP_ERR_PATTERN, get_filename, get_media_filename,
                   get_subset_name, is_url, run_command)

LOGGER = logging.getLogger('yt-downloader')
LOGGER.setLevel(logging.INFO)

# set global path for ffmpeg and ffprobe
GL_FFMPEG = '/opt/homebrew/bin/ffmpeg'
GL_FFPROBE = '/opt/homebrew/bin/ffprobe'


def parse_arguments():
    """
    Parse arguments from the command line


    Returns:
        args:  Argument dictionary
               (Type: dict[str, str])
    """
    parser = argparse.ArgumentParser(
        description='Download AudioSet data locally')

    parser.add_argument('-i',
                        '--input',
                        dest='input_meta',
                        required=True,
                        default='meta.csv',
                        type=str,
                        help='Input meta file')

    parser.add_argument('-o',
                        '--output',
                        dest='output_dst',
                        required=True,
                        default='./data',
                        type=str,
                        help='Input meta file')

    parser.add_argument('-f',
                        '--ffmpeg',
                        dest='ffmpeg_path',
                        action='store',
                        type=str,
                        default=GL_FFMPEG,
                        help='Path to ffmpeg executable')

    parser.add_argument('-fp',
                        '--ffprobe',
                        dest='ffprobe_path',
                        action='store',
                        type=str,
                        default=GL_FFPROBE,
                        help='Path to ffprobe executable')

    parser.add_argument('-ac',
                        '--audio-codec',
                        dest='audio_codec',
                        action='store',
                        type=str,
                        default='pcm_s16le',
                        help='Name of audio codec used by ffmpeg to encode output audio')

    parser.add_argument('-asr',
                        '--audio-sample-rate',
                        dest='audio_sample_rate',
                        action='store',
                        type=int,
                        default=48000,
                        help='Target audio sample rate (in Hz)')

    parser.add_argument('-abd',
                        '--audio-bit-depth',
                        dest='audio_bit_depth',
                        action='store',
                        type=int,
                        default=16,
                        help='Target audio sample bit depth')

    parser.add_argument('-vc',
                        '--video-codec',
                        dest='video_codec',
                        action='store',
                        type=str,
                        default='h264',
                        help='Name of video codec used by ffmpeg to encode output audio')

    parser.add_argument('-af',
                        '--audio-format',
                        dest='audio_format',
                        action='store',
                        type=str,
                        default='wav',
                        help='Name of audio format used by ffmpeg for output audio')

    parser.add_argument('-vf',
                        '--video-format',
                        dest='video_format',
                        action='store',
                        type=str,
                        default='mp4',
                        help='Name of video format used by ffmpeg for output video')

    parser.add_argument('-vm',
                        '--video-mode',
                        dest='video_mode',
                        action='store',
                        type=str,
                        default='bestvideo',
                        help="Name of the method in which video is downloaded. "
                             "'bestvideo' obtains the best quality video that "
                             "does not contain an audio stream. 'bestvideoaudio' "
                             "obtains the best quality video that contains an "
                             "audio stream. 'bestvideowithaudio' obtains the "
                             "best quality video without an audio stream and "
                             " merges it with audio stream")

    parser.add_argument('-vfr',
                        '--video-frame-rate',
                        dest='video_frame_rate',
                        action='store',
                        type=int,
                        default=30,
                        help='Target video frame rate (in fps)')

    parser.add_argument('-nr',
                        '--num-retries',
                        dest='num_retries',
                        action='store',
                        type=int,
                        default=10,
                        help='Number of retries when ffmpeg encounters an HTTP'
                             'issue, which could be to unpredictable network behavior')
    parser.add_argument('-n',
                        '--num-workers',
                        dest='num_workers',
                        action='store',
                        type=int,
                        default=6,
                        help='Number of multiprocessing workers used to download videos')

    parser.add_argument('-nl',
                        '--no-logging',
                        dest='disable_logging',
                        action='store_true',
                        default=False,
                        help='Disables logging if flag enabled')

    parser.add_argument('-lp',
                        '--log-path',
                        dest='log_path',
                        action='store',
                        default=None,
                        help='Path to log file generated by this script. '
                             'By default, the path is "./audiosetdl.log".')

    parser.add_argument('-v',
                        '--verbose',
                        dest='verbose',
                        action='store_true',
                        default=False,
                        help='Prints verbose info to stdout')

    return vars(parser.parse_args())


def ffmpeg(ffmpeg_path, input_path, output_path, input_args=None,
           output_args=None, log_level='error', num_retries=10,
           validation_callback=None, validation_args=None):

    if type(input_path) == str:
        inputs = ['-i', input_path]
    elif isinstance(input_path, collections.Iterable):
        inputs = []
        for path in input_path:
            inputs.append('-i')
            inputs.append(path)
    else:
        error_msg = '"input_path" must be a str or an iterable, but got type {}'
        raise ValueError(error_msg.format(str(type(input_path))))

    if not input_args:
        input_args = []
    if not output_args:
        output_args = []

    last_err = None
    for attempt in range(num_retries):
        try:
            args = [ffmpeg_path] + input_args + inputs + \
                output_args + [output_path, '-loglevel', log_level]
            run_command(args)

            # Validate if a callback was passed in
            if validation_callback is not None:
                validation_args = validation_args or {}
                validation_callback(output_path, **validation_args)
            break
        except SubprocessError as e:
            last_err = e
            stderr = e.cmd_stderr.rstrip()
            if stderr.endswith('already exists. Exiting.'):
                LOGGER.info(
                    'ffmpeg output file "{}" already exists.'.format(output_path))
                break
            elif HTTP_ERR_PATTERN.match(stderr):
                # Retry if we got a 4XX or 5XX, in case it was just a network issue
                continue

            LOGGER.error(str(e) + '. Retrying...')
            if os.path.exists(output_path):
                os.remove(output_path)

        except FfmpegIncorrectDurationError as e:
            last_err = e
            if attempt < num_retries - 1 and os.path.exists(output_path):
                os.remove(output_path)
            # If the duration of the output audio is different, alter the
            # duration argument to account for this difference and try again
            duration_diff = e.target_duration - e.actual_duration
            try:
                duration_idx = input_args.index('-t') + 1
                input_args[duration_idx] = str(
                    float(input_args[duration_idx]) + duration_diff)
            except ValueError:
                duration_idx = output_args.index('-t') + 1
                output_args[duration_idx] = str(
                    float(output_args[duration_idx]) + duration_diff)

            LOGGER.warning(str(e) + '; Retrying...')
            continue

        except FfmpegValidationError as e:
            last_err = e
            if attempt < num_retries - 1 and os.path.exists(output_path):
                os.remove(output_path)
            # Retry if the output did not validate
            LOGGER.info('ffmpeg output file "{}" did not validate: {}. Retrying...'.format(
                output_path, e))
            continue
    else:
        error_msg = 'Maximum number of retries ({}) reached. Could not obtain inputs at {}. Error: {}'
        LOGGER.error(error_msg.format(num_retries, input_path, str(last_err)))


def download_yt_video(ytid, ts_start, ts_end, output_dir, ffmpeg_path, ffprobe_path,
                      audio_codec='flac', audio_format='flac',
                      audio_sample_rate=48000, audio_bit_depth=16,
                      video_codec='h264', video_format='mp4',
                      video_mode='bestvideo', video_frame_rate=30,
                      num_retries=10):

    # Compute some things from the segment boundaries
    duration = 10

    # Make the output format and video URL
    # Output format is in the format:
    #   <YouTube ID>_<start time in ms>_<end time in ms>.<extension>
    media_filename = get_media_filename(ytid, ts_start, ts_end)
    videowithaudio_filepath = os.path.join(
        output_dir, 'video_audio', media_filename + '.' + video_format)
    video_filepath = os.path.join(
        output_dir, 'video', media_filename + '.' + video_format)
    audio_filepath = os.path.join(
        output_dir, 'audio', media_filename + '.' + audio_format)

    # Get the direct URLs to the videos with best audio and with best video (with audio)
    if os.path.exists(videowithaudio_filepath):
        os.remove(videowithaudio_filepath)

    if os.path.exists(video_filepath):
        os.remove(video_filepath)

    if os.path.exists(audio_filepath):
        os.remove(audio_filepath)

    if len(ytid) != 11:
        print(ytid)
        return None, None

    videoFormats = Video.getFormats(ytid)
    url = videoFormats['streamingData']['formats'][-1]['url']

    audio_info = {
        'sample_rate': audio_sample_rate,
        'channels': 2,
        'bitrate': audio_bit_depth,
        'encoding': audio_codec.upper(),
        'duration': duration
    }
    video_info = {
        "r_frame_rate": "{}/1".format(video_frame_rate),
        "avg_frame_rate": "{}/1".format(video_frame_rate),
        'codec_name': video_codec.lower(),
        'duration': duration
    }

    # Download the audio
    audio_input_args = ['-n', '-ss', str(ts_start)]
    audio_output_args = ['-t', str(duration),
                         '-ar', str(audio_sample_rate),
                         '-vn',
                         '-ac', str(audio_info['channels']),
                         '-sample_fmt', 's{}'.format(audio_bit_depth),
                         '-f', audio_format,
                         '-acodec', audio_codec]

    # if not os.path.exists(audio_filepath):
    ffmpeg(ffmpeg_path, url, audio_filepath,
           input_args=audio_input_args, output_args=audio_output_args,
           num_retries=num_retries)

    # Download the best quality video, in lossless encoding
    if video_codec != 'h264':
        error_msg = 'Not currently supporting merging of best quality video with video for codec: {}'
        raise NotImplementedError(error_msg.format(video_codec))
    video_input_args = ['-n', '-ss', str(ts_start)]
    video_output_args = ['-t', str(duration),
                         '-f', video_format,
                         '-crf', '0',
                         '-preset', 'medium',
                         '-r', str(video_frame_rate),
                         '-an',
                         '-vcodec', video_codec]

    # if not os.path.exists(video_filepath):
    ffmpeg(ffmpeg_path, url, video_filepath,
           input_args=video_input_args, output_args=video_output_args,
           num_retries=num_retries)

    # Merge the best lossless video with the lossless audio, and compress
    video_input_args = ['-n']
    video_output_args = ['-f', video_format,
                         '-r', str(video_frame_rate),
                         '-vcodec', video_codec,
                         '-acodec', 'aac',
                         '-ar', str(audio_sample_rate),
                         '-ac', str(audio_info['channels']),
                         '-strict', 'experimental']

    ffmpeg(ffmpeg_path, [video_filepath, audio_filepath], videowithaudio_filepath,
           input_args=video_input_args, output_args=video_output_args,
           num_retries=num_retries)

    LOGGER.info('Downloaded video {} ({} - {})'.format(ytid, ts_start, ts_end))

    return video_filepath, audio_filepath


def segment_mp_worker(ytid, ts_start, ts_end, data_dir, ffmpeg_path,
                      ffprobe_path, **ffmpeg_cfg):

    LOGGER.info(
        'Attempting to download video {} ({} - {})'.format(ytid, ts_start, ts_end))

    # Download the video
    try:
        download_yt_video(ytid, ts_start, ts_end, data_dir, ffmpeg_path,
                          ffprobe_path, **ffmpeg_cfg)
    except SubprocessError as e:
        err_msg = 'Error while downloading video {}: {}; {}'.format(
            ytid, e, tb.format_exc())
        LOGGER.error(err_msg)
    except Exception as e:
        err_msg = 'Error while processing video {}: {}; {}'.format(
            ytid, e, tb.format_exc())
        LOGGER.error(err_msg)


def download_subset_videos(subset_path, data_dir, ffmpeg_path, ffprobe_path,
                           num_workers, **ffmpeg_cfg):

    subset_name = get_subset_name(subset_path)

    LOGGER.info('Starting download jobs for subset "{}"'.format(subset_name))
    with open(subset_path, 'r') as f:
        subset_data = csv.reader(f)

        # Set up multiprocessing pool
        pool = mp.Pool(num_workers)
        try:
            for row_idx, row in enumerate(subset_data[1:]):
                # Skip commented lines

                ytid, ts_start = row[0], float(
                    int(row[1]) * 3600 + int(row[2]) * 60 + int(row[3]))
                ts_end = ts_start + float(10)

                # Skip files that already have been downloaded
                media_filename = get_media_filename(ytid, ts_start, ts_end)
                videowithaudio_filepath = os.path.join(
                    data_dir, 'video_audio', media_filename + '.' + ffmpeg_cfg.get('video_format', 'mp4'))
                video_filepath = os.path.join(
                    data_dir, 'video', media_filename + '.' + ffmpeg_cfg.get('video_format', 'mp4'))
                audio_filepath = os.path.join(
                    data_dir, 'audio', media_filename + '.' + ffmpeg_cfg.get('audio_format', 'wav'))

                if os.path.exists(video_filepath) and os.path.exists(audio_filepath) and os.path.exists(videowithaudio_filepath):
                    info_msg = 'Already downloaded video {} ({} - {}). Skipping.'
                    LOGGER.info(info_msg.format(ytid, ts_start, ts_end))
                    continue

                worker_args = [ytid, ts_start, ts_end,
                               data_dir, ffmpeg_path, ffprobe_path]
                pool.apply_async(
                    partial(segment_mp_worker, **ffmpeg_cfg), worker_args)
                # Run serially
                #segment_mp_worker(*worker_args, **ffmpeg_cfg)

        except csv.Error as e:
            err_msg = 'Encountered error in {} at line {}: {}'
            LOGGER.error(err_msg)
            sys.exit(err_msg.format(subset_path, row_idx+1, e))
        except KeyboardInterrupt:
            LOGGER.info("Forcing exit.")
            exit()
        finally:
            try:
                pool.close()
                pool.join()
            except KeyboardInterrupt:
                LOGGER.info("Forcing exit.")
                exit()

    LOGGER.info('Finished download jobs for subset "{}"'.format(subset_name))


def download(input_meta, output_dst, ffmpeg_path, ffprobe_path, disable_logging=False,
             verbose=False, num_workers=4, log_path=None, **ffmpeg_cfg):

    init_console_logger(LOGGER, verbose=verbose)
    if not disable_logging:
        init_file_logger(LOGGER, log_path=log_path)
    multiprocessing_logging.install_mp_handler()
    LOGGER.debug('Initialized logging.')
    
    assert os.path.exists(output_dst), 'output dir does not exist!'
    os.makedirs(os.path.join(output_dst, 'audio'), exist_ok=True)
    os.makedirs(os.path.join(output_dst, 'video'), exist_ok=True)
    os.makedirs(os.path.join(output_dst, 'video_audio'), exist_ok=True)

    download_subset_videos(input_meta, output_dst, ffmpeg_path,
                           ffprobe_path, num_workers, **ffmpeg_cfg)


if __name__ == '__main__':
    # TODO: Handle killing of ffmpeg (https://stackoverflow.com/questions/6488275/terminal-text-becomes-invisible-after-terminating-subprocess)
    #       so we don't have to use this hack
    atexit.register(lambda: os.system('stty sane')
                    if sys.stdin.isatty() else None)
    download(**parse_arguments())
