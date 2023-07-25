#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import enum
import json
import time
import shlex
import shutil
import logging
import paramiko
import threading
import traceback
import subprocess
import ffmpeg_parser
from io import StringIO
from decimal import Decimal
from serde import deserialize, field
from serde.json import from_json, to_json
from dataclasses import dataclass, fields
from typing import Optional, Dict, List, Tuple, Callable


class LogLevel(enum.Enum):
    ERROR = 40
    WARNING = 30
    QUIET = 25
    INFO = 20
    VERBOSE = 15
    DEBUG = 10
    SHELL = 5

for l in LogLevel:
    logging.addLevelName(l.value, l.name)

logging.basicConfig(format='[%(levelname)-8s] (%(name)s > %(funcName)s) %(message)s')
logger = logging.getLogger('DistrFFmpeg')


class DistrFFmpegError(Exception):
    pass


@dataclass
class CommandResult:

    stdout: bytes
    stderr: bytes
    exit_code: int


@dataclass
class Job:

    ffmpeg_cmds: List[ffmpeg_parser.FFmpegCommand]
    segments_dir: str
    required_segments: List[str]
    output_fpath: str
    taken: bool = False
    completed: bool = False
    retries: int = -1


@deserialize
@dataclass
class Worker:

    user: str
    host: str
    work_path: str
    ffmpeg_bin: str
    params: Dict[str, str]
    platform: str = 'Linux'  # dirty: maybe make a separate type for it

    free: bool = True
    ssh: Optional[paramiko.client.SSHClient] = None
    sftp: Optional[paramiko.sftp_client.SFTPClient] = None
    connected: bool = False

    jobs_completed: int = 0
    exec_command: Optional[Callable] = None

    def connect(self) -> None:

        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()

        try:
            self.ssh.connect(self.host, username=self.user, **self.params)
        except:
            # if unable to connect to worker via ssh
            # just set connected to False
            # TODO: inform about worker being offline
            self.connected = False
            return

        self.sftp = self.ssh.open_sftp()
        self.connected = True
        
        if self.platform not in ('Linux', 'Windows'):
            return DistrFFmpegError("Worker platform has to be either Linux or Windows.")
        self.exec_command = self.exec_command_linux if self.platform == 'Linux' else self.exec_command_windows

    def disconnect(self) -> None:
        self.sftp.close()
        self.ssh.close()
        self.sftp = None
        self.ssh = None
        self.connected = False

    def exec_command_linux(self, cmd: str) -> CommandResult:
        assert self.connected
        logger.log(LogLevel.SHELL.value, f"Running remote Linux command: {cmd}")

        stdin, stdout, stderr = self.ssh.exec_command(cmd, get_pty=True)
        exit_code = stdout.channel.recv_exit_status()
        stdin.close()
        return CommandResult(stdout.read(), stderr.read(), exit_code)

    # on windows command is executed via powershell
    # the client is required to have powershell installed
    def exec_command_windows(self, cmd: str) -> CommandResult:
        assert self.connected
        logger.log(LogLevel.SHELL.value, f"Running remote Windows command: {cmd}")
        
        # check if powershell is installed on distrffmpeg windows client
        # don't check version for now but in the future we might need to
        stdin, stdout, stderr = self.ssh.exec_command('powershell -command "$PSVersionTable.PSVersion.Major"')
        stdin.close()
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            raise DistrFFmpegError("Windows worker does not have powershell installed.")
        
        stdin, stdout, stderr = self.ssh.exec_command("powershell -noprofile -noninteractive -")
        # in powershell when command starts with a '"' it's interpreted as string
        # you have to add '&' at the beginning in order to execute the command
        stdin.write('& ' + cmd)
        stdin.flush()
        stdin.close()
        exit_code = stdout.channel.recv_exit_status()
        
        return CommandResult(stdout.read(), stderr.read(), exit_code)

    def add_job(self, job: Job) -> None:

        self.free = False
        job.taken = True
        
        job.retries += 1

        try:
            self._add_job_supervised(job)
        except:
            # job failed
            # TODO: print verbose info about failed job
            traceback.print_exc()
            self.free = True
            job.taken = False
            return

        job.completed = True
        self.free = True

        self.jobs_completed += 1


    def _add_job_supervised(self, job: Job) -> None:

        self.job_work_path = os.path.join(
            self.work_path, os.urandom(8).hex()
        )

        res = self.exec_command(f'mkdir -p "{self.job_work_path}"')
        assert res.exit_code == 0

        sio = StringIO()
        for seg in job.required_segments:
            self.sftp.put(os.path.join(job.segments_dir, seg.filename), os.path.join(self.job_work_path, seg.filename))
            sio.write(f"file '{seg.filename}'\n")

        self.sftp.put(os.path.join(job.segments_dir, "segments.csv"), os.path.join(self.job_work_path, "segments.csv"))
        sio.seek(0)
        self.sftp.putfo(sio, os.path.join(self.job_work_path, "segments.txt"))
        sio.close()

        shell_cmd = '; '.join([self.ffmpeg_bin + " " + cmd.get_command(without_bin=True) for cmd in job.ffmpeg_cmds])
        res = self.exec_command(f'cd "{self.job_work_path}"; {shell_cmd}')
        logger.log(LogLevel.SHELL.value, f'Executed shell command: {shell_cmd}')
        # TODO: also show output of commands in log
        #logger.debug(res.stdout.decode("utf-8"), res.stderr.decode("utf-8"))
        assert res.exit_code == 0
        
        self.sftp.get(os.path.join(self.job_work_path, "out.mkv"), job.output_fpath)
        self.exec_command(
            ('rm -rf' if self.platform == 'Linux' else 'rm -r -force') + f' "{self.job_work_path}"'
        )


@deserialize
@dataclass
class Config:

    ffmpeg_bin: str
    ffprobe_bin: str
    server_work_path: str
    segment_frames: int
    keyint_min: int
    keyint_max: int
    job_max_retries: int
    loglevel: LogLevel = field(deserializer=lambda x: LogLevel[x])
    workers: List[Worker]

    # this will not check if provided ffmpeg/ffprobe binaries exist
    def validate(self) -> bool:

        is_valid = (
            self.segment_frames > 0 and
            self.keyint_min > 0 and
            self.keyint_max >= self.keyint_min and
            self.job_max_retries > 0 and
            self.workers != []
        )

        return is_valid


@dataclass
class SceneScore:

    frame: Optional[int] = None
    pts: Optional[int] = None
    pts_time: Optional[Decimal] = None
    score: Optional[float] = None


@dataclass
class Segment:

    idx: Optional[int] = None
    filename: Optional[str] = None
    frame_range: Optional[range] = None
    first_keyframe: Optional[int] = None


class DistrFFmpeg(object):


    config: Optional[Config]
    cur_work_path: Optional[str]
    scene_scores: List[SceneScore]
    segments: List[Segment]
    jobs: List[Job]
    user_cmd: str

    def __init__(self, config_path: str, raw_user_cmd: str) -> None:

        # read config file
        with open(config_path, 'rb') as f:
            self.config = from_json(Config, f.read())

        # override config values from those user provided in command line
        # sets self.user_cmd to the processed user command
        self.prepare_user_cmd(raw_user_cmd)

        if not self.config.validate():
            raise DistrFFmpegError('Invalid parameter in config')

        logger.setLevel(self.config.loglevel.value)

        logger.log(LogLevel.INFO.value, f'Parsed config file from: {config_path}')
        logger.log(LogLevel.SHELL.value, f'Parsed input command line: {self.user_cmd}')

        self.cur_work_path = None
        self.scene_scores = []
        self.segments = []
        self.jobs = []

        self.fetch_cur_work_path()


    def execute_shell(self, cmd: str) -> subprocess.CompletedProcess:

        logger.log(LogLevel.SHELL.value, f"Running local command: {cmd}")
        r = subprocess.run(cmd, capture_output=True, shell=True)

        if r.returncode != 0:
            logger.log(LogLevel.SHELL.value, f"Failed command stdout: {r.stdout.decode('utf-8')}")
            logger.log(LogLevel.SHELL.value, f"Failed command stderr: {r.stderr.decode('utf-8')}")
            raise DistrFFmpegError("Command finished with non-zero exit code")

        return r


    def get_ffmpeg_commands(self, base_cmd: str, scope: ffmpeg_parser.ArgScope) -> List[ffmpeg_parser.FFmpegCommand]:

        parser = ffmpeg_parser.Parser(scope)

        parser.parse_command(base_cmd, template=True)
        parser.parse_command(self.user_cmd)

        return parser.cmds


    def run(self) -> None:

        #self.fetch_parser()

        pre_start_time = time.time()

        self.fetch_scenescores()
        self.fetch_segments()

        self.fetch_jobs()

        workers_cnt = 0

        # connect to each worker
        for worker in self.config.workers:

            worker.connect()
            logger.log(
                LogLevel.DEBUG.value,
                f'Connection to {worker.user}@{worker.host} -> ' + ('ok' if worker.connected else 'failed')
            )

            if worker.connected:
                workers_cnt += 1

        if workers_cnt == 0:
            raise DistrFFmpegError('No workers found online.')

        logger.log(LogLevel.QUIET.value, f'Starting distributed encoder with {workers_cnt} workers.')

        start_time = time.time()

        # while not all jobs have completed state
        while True:

            completed_jobs_count = [j.completed for j in self.jobs].count(True)
            logger.log(LogLevel.INFO.value, f'Completed jobs: {completed_jobs_count} / {len(self.jobs)}')

            if completed_jobs_count == len(self.jobs):
                break

            for worker in self.config.workers:
                if worker.free and worker.connected:
                    j = self.get_waiting_job()
                    
                    if not j:
                        break

                    if j.retries >= self.config.job_max_retries:
                        raise DistrFFmpegError("Exceeded number of retries for current job.")

                    t = threading.Thread(target=worker.add_job, args=(j,))
                    t.daemon = True
                    t.start()

            time.sleep(5)

        self.merge_final_slices()

        time_taken_total = round(time.time() - pre_start_time, 2)
        time_taken = round(time.time() - start_time, 2)
        logger.log(LogLevel.QUIET.value, f"Total time: {time_taken_total} seconds.")
        logger.log(LogLevel.QUIET.value, f"Encoding time: {time_taken} seconds.")

        # calculate how much each host contributed
        host_shares = {}
        for worker in self.config.workers:

            if not worker.host in host_shares.keys():
                host_shares[worker.host] = 0

            host_shares[worker.host] += worker.jobs_completed / len(self.jobs)

        logger.log(LogLevel.INFO.value, f'Host shares: {host_shares}')


    def merge_final_slices(self) -> None:

        logger.log(LogLevel.INFO.value, 'Merging slices into final video.')

        slices_dir = os.path.join(self.cur_work_path, "slices_final")
        slices_metafile_path = os.path.join(slices_dir, "slices.txt")

        with open(slices_metafile_path, 'w', encoding='utf-8') as f:
            for job in self.jobs:
                slice_fname = os.path.basename(job.output_fpath)
                f.write(f"file '{slice_fname}'\n")

        # merge final slices and add audio from input video to them
        # dirty: assumes input video has one audio track
        ffcmds = self.get_ffmpeg_commands(f'ffmpeg -y -f concat -safe 0 -i "{slices_metafile_path}" -i PLACEHOLDER -c:v copy -map 0:v:0 -map 1:a:0 ', ffmpeg_parser.ArgScope.FINAL)
        shell_cmd = self.config.ffmpeg_bin + " " + ffcmds[-1].get_command(without_bin=True)
        self.execute_shell(shell_cmd)

        # clean up host's working directory
        shutil.rmtree(self.cur_work_path)


    def get_waiting_job(self) -> Optional[Job]:

        for job in self.jobs:
            if not job.taken:
                return job
        return None


    def fetch_scenescores(self) -> None:

        logger.log(LogLevel.INFO.value, 'Fetching scene scores.')

        self.scene_scores = []
        
        scenescores_path = os.path.join(self.cur_work_path, 'scenescores.txt')

        ffcmds = self.get_ffmpeg_commands(f'ffmpeg -y -i PLACEHOLDER -vf "select=\'gte(scene,0)\',metadata=print:file=\'{scenescores_path}\'" -f null -', ffmpeg_parser.ArgScope.PRE)

        shell_cmd = self.config.ffmpeg_bin + " " + ffcmds[0].get_command(without_bin=True)
        self.execute_shell(shell_cmd)
        with open(scenescores_path, 'r', encoding='utf-8') as f:

            while True:
                cur_score = SceneScore()

                frameinfo = f.readline().strip()
                if frameinfo == '':
                    break

                for keyval in frameinfo.split(' '):
                    if keyval == '':
                        continue
                    keyval = keyval.strip()
                    key, val = keyval.split(':')
                    setattr(cur_score, key, Decimal(val) if '.' in val else int(val))

                lavfi_ss = f.readline().strip()
                cur_score.score = float(lavfi_ss.split('=')[1])
                self.scene_scores.append(cur_score)


    def fetch_segments(self) -> None:

        self.segments = []

        os.makedirs(self.segments_dir, exist_ok=True)
        segments_meta_path = os.path.join(self.segments_dir, 'segments.csv')
        self.segments = [
            Segment(idx=i, filename='out{}.mkv'.format(str(i).zfill(6)),
            frame_range=range(i*self.config.segment_frames, (i+1)*self.config.segment_frames)) \
            for i in range((len(self.scene_scores)//self.config.segment_frames)+1)
        ]
        # don't include 0th frame as it causes trouble with ffmpeg
        segment_frames_arg = ','.join([str(seg.frame_range[0]) for seg in self.segments[1:]])
        #print(self.segments, segment_frames_arg)

        ffcmds = self.get_ffmpeg_commands(f'ffmpeg -y -i PLACEHOLDER -c copy -f segment -segment_frames {segment_frames_arg} -segment_list "{segments_meta_path}" -reset_timestamps 1 -break_non_keyframes 1 "{self.segments_dir}/out%06d.mkv"', ffmpeg_parser.ArgScope.PRE)

        shell_cmd = self.config.ffmpeg_bin + " " + ffcmds[0].get_command(without_bin=True)
        self.execute_shell(shell_cmd)
        
        # run ffprobe on every segment in order to get first keyframe index
        for seg in self.segments:

            r = self.execute_shell(f'"{self.config.ffprobe_bin}" -select_streams v -print_format json -show_packets "{self.segments_dir}/{seg.filename}"')
            packets = json.loads(r.stdout).get('packets')
            
            pkt_idx = 0
            for packet in packets:
                if packet.get('codec_type') != "video":
                    continue
                if packet.get('flags').startswith('K'):
                    seg.first_keyframe = pkt_idx
                    break
                pkt_idx += 1


    def fetch_jobs(self) -> None:

        assert self.config
        assert self.segments
        assert self.scene_scores

        #print(self.segments)

        self.jobs = []
        
        slices_dir = os.path.join(self.cur_work_path, "slices_final")
        os.makedirs(slices_dir, exist_ok=True)
        
        cur_frame_idx = 0
        slice_idx = 0

        while True:
            cur_slice = self.scene_scores[
                cur_frame_idx+self.config.keyint_min:cur_frame_idx+self.config.keyint_max
            ]
            
            # range correction frame: 'select' filter of ffmpeg treats frames inclusively,
            # last frame gets repeated with the first of next segment
            # we don't wont that unless it's the last frame of video
            range_correction_frame = 1
            if cur_slice:
                cur_split_frame = max(cur_slice, key=lambda x: x.score)
            else:
                range_correction_frame = 0
                cur_split_frame = self.scene_scores[-1]
            
            first_frame = self.scene_scores[cur_frame_idx]
            last_frame = cur_split_frame

            # find the first segment which needs to be sent to remote worker
            # the worker must be able to decode the video from segment
            # so the first segment needs to have an keyframe in it
            first_segment_idx = self.get_segment_at_frame(first_frame.frame).idx
            while True:
                first_segment_idx = max(first_segment_idx-1, 0)
                if self.segments[first_segment_idx].first_keyframe is not None:
                    break
            # bug fixed :)

            # TODO: fixed constant of segment lookahead == 2
            # do something more flexible??
            required_segments = self.segments[first_segment_idx:self.get_segment_at_frame(last_frame.frame).idx+2]
            rel_startframe = first_frame.frame - (required_segments[0].frame_range[0] + required_segments[0].first_keyframe)
            rel_endframe = (last_frame.frame - (required_segments[0].frame_range[0] + required_segments[0].first_keyframe)) - range_correction_frame

            #print(f"slice {slice_idx}:", rel_startframe, rel_endframe)
            # -ss 0.0 fixes bugs in some codecs like vp9 but breaks things

            ffcmds = self.get_ffmpeg_commands(f'ffmpeg -y -f concat -safe 0 -i segments.txt -vf "select=between(n\,{rel_startframe}\,{rel_endframe}),setpts=N/FRAME_RATE/TB" -fps_mode passthrough -frame_pts true -an -g 10000 out.mkv', ffmpeg_parser.ArgScope.REMOTE)

            output_path = os.path.join(slices_dir, str(slice_idx).zfill(6)+".mkv")
            
            self.jobs.append(Job(ffcmds, self.segments_dir, required_segments, output_path))
            
            if not cur_slice:
                break
            
            cur_frame_idx = cur_split_frame.frame
            slice_idx += 1


    def get_segment_at_frame(self, frame: int) -> Optional[Segment]:

        for seg in self.segments:
            if frame in seg.frame_range:
                return seg
        
        return None


    def fetch_cur_work_path(self) -> None:
        self.cur_work_path = os.path.join(
            self.config.server_work_path, os.urandom(8).hex()
        )
        os.makedirs(self.cur_work_path, exist_ok=True)
        self.segments_dir = os.path.join(self.cur_work_path, 'segments')


    def prepare_user_cmd(self, raw_user_cmd: str) -> None:

        def _process_arg(arg: str) -> str:

            # search for distrffmpeg config arguments
            if arg.startswith('-df_') and arg.count('=') == 1:
                name, val = arg[4:].split('=')

                field_type = self.config.__annotations__.get(name)
                if field_type is None:
                    raise DistrFFmpegError(f"Overrided config field {name} does not exist.")

                if field_type not in (str, int, "str", "int"):
                    raise DistrFFmpegError(f"Trying to override config field of type {field_type} is not allowed.")

                # dirty: hacky
                # in python 3.10+ without importing annotations setting field_type
                # will return class type
                # in python 3.7..3.9 you need to import annotations from __future__
                # returned value for field_type will be a string like "int", "str"
                # so it would need to be converted into class type
                if not isinstance(field_type, type):
                    field_type = getattr(__builtins__, field_type)

                self.config.__setattr__(name, field_type(val))

                # remove this argument from ffmpeg command
                return ""

            return shlex.quote(arg)

        # simulate the program name to be ffmpeg and quote necessary arguemnts
        self.user_cmd = "ffmpeg " + " ".join(map(_process_arg, raw_user_cmd[1:]))


if __name__ == '__main__':

    config_path = os.path.join(os.path.expanduser("~/.config"), "distrffmpeg", "config.json")

    if not os.path.exists(config_path):
        raise DistrFFmpegError(f"Please create a config file and place it in: {config_path}.")

    df = DistrFFmpeg(config_path, sys.argv)

    df.run()
