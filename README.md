# DistrFFmpeg

A script that breaks down video encoding into chunks and spreads them across multiple machines accessible via SSH.


## Installation

The following setup requires: `git python3 python3-pip ssh ffmpeg`, install those packages according to your package manager.

> **WARNING**: The minimum supported version of FFmpeg is 5.1 due to the use of [`-fps_mode`](https://github.com/FFmpeg/FFmpeg/commit/09c53a04c5892baee88872fbce3df17a00472faa) parameter. You can easily obtain latest FFmpeg version from [ffmpeg.org](https://ffmpeg.org)

```shell
$ git clone https://github.com/olokelo/distrffmpeg.git
$ cd distrffmpeg
$ pip install -r requirements.txt
```

You can now add the distrffmpeg directory to `PATH` so that `distrffmpeg.py` can be executed from any directory.

```shell
$ export PATH="$PATH:$(pwd)"
```

## Usage

First of all you need to create configuration file for DistrFFmpeg in `~/.config/distrffmpeg/config.json`. Just copy the `config.sample.json` and tweak it.

```shell
$ mkdir -p ~/.config/distrffmpeg
$ cp config.sample.json ~/.config/distrffmpeg/config.json
$ nano ~/.config/distrffmpeg/config.json
```

Now make sure to copy SSH id of host to each worker so that you can login to all of the provided workers without password.
Do that for every worker you specified in `config.json`:

```shell
$ ssh-keygen
$ ssh-copy-id user@host.local
```

> Not all workers need to be online. DistrFFmpeg will automatically discard offline workers but it needs at least one reachable worker. 

DistrFFmpeg can now be executed with the same command line syntax as the regular FFmpeg. It will get all neccessary parameters from the config file.

```shell
$ distrffmpeg.py -i video.mp4 -c:v libsvtav1 -preset 7 -crf 52 -c:a libopus -b:a 64k video.distrav1.webm 
```

However if you want to override some DistrFFmpeg parameter, you can provide a parameter followed by `-df_`.
```shell
$ distrffmpeg.py -df_keyint_min=600 -df_keyint_max=1200 -df_segment_frames=200 -i video.mp4 -c:v libsvtav1 -preset 7 -crf 52 -c:a libopus -b:a 64k video.distrav1.webm 
```

## How does it work?

### Terminology

- **Host** - A computer that has DistrFFmpeg installed and the entire source video file available.
- **Worker** - A computer accessible by the Host via SSH without password login. It needs to have FFmpeg installed.
- **Config** - A config file for DistrFFmpeg located on the Host at `~/.config/distrffmpeg/config.json`.
- **Chunk || Segment** - A part of source video that was split on the Host.
- **Final Chunk || Final Segment** - The resulting encoded part of source video that is sent from Worker to Host.

When you run DistrFFmpeg it parses the command line  (`ffmpeg_parser.py`) and creates individual FFmpeg commands out of it.

Commands are divided into 3 stages:
- **PRE** - The command that is being run on the Host to analyze source video file and split it discarding audio stream at the same time.
- **REMOTE** - A set of commands that will be executed on remote Workers. There can be more than one command executed for each chunk for example in case of 2-pass encoding (not well tested).
- **FINAL** - The command that runs at the end of encoding when Host had received all Final Segments. It merges them into a final encoded video file and compresses audio.

DistrFFmpeg runs the PRE command and then initializes connection to available Workers. It sends them Chunks that are needed to encode one Final Segment via SFTP.

Then it executes all REMOTE commands for a specific Final Segment on given Worker using SSH and retrieves the output file if the Worker had finished.

When all Final Segments are encoded and retrieved by the Host, it runs FINAL command to merge them into a final encoded video.


## How is it different from Av1an?

DistrFFmpeg will never replace [Av1an](https://github.com/master-of-zen/Av1an). It is just a simple script for tracking encoding process across multiple machines and merging encoded clips into final video.

Here are some key differences:

- Av1an is [more complex](https://github.com/master-of-zen/Av1an#features) and allows you to target certain VMAF, input VapourSynth scripts, resume progress etc.

- Av1an can't encode on multiple machines at once ([see this issue](https://github.com/master-of-zen/Av1an/issues/406)). That's the whole purpose of DistrFFmpeg. However if you start SSH server on your local machine and include only `user@localhost` as a worker, DistrFFmpeg can work just like Av1an on your local machine.

- DistrFFmpeg works on any host machine that has Python, SSH client and FFmpeg installed. No need for VapourSynth or standalone encoders.

  Workers need to only have SSH server running and FFmpeg installed. (Powershell is also required on Windows workers)

- DistrFFmpeg uses some nasty FFmpeg hacks. That might lead to workers not being able to decode partial chunks using certain codecs (like VP9). The main problem is that splited chunks may not start with a keyframe.


