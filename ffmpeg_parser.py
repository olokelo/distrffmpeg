from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from pprint import pprint
from copy import deepcopy
import shlex


# in the command "ffmpeg -i input.mp4"
# the "-i" is called a flag
# and "input.mp4" is called an argument


# selects the encoding stage at which should the specific argument be applied
class ArgScope(Enum):
    PRE     = 1
    REMOTE  = 2
    FINAL   = 3
    DISCARD = 4
    UNKNOWN = 5

class ArgPolicy(Enum):
    CONCAT   = 1
    MULTIPLE = 2
    FORBID   = 3

ARG_SCOPES: Dict[ArgScope, List[Optional[str]]] = \
{
    ArgScope.PRE: [
        "threads", "ss", "t", "frames:v", "vframes", "i"
    ],
    ArgScope.REMOTE: [
        "c:v", "vcodec", "crf", "qp", "b:v", "vn", "pass", "filter:v", "vf", "f",
    ],
    ArgScope.FINAL: [
        "c:a", "acodec", "b:a", "an", "movflags", "i", None
    ],
    ArgScope.DISCARD: [
        "y", "n", "v", "loglevel", "report", "g"
    ]
}

# all others will default to being updated/replaced
ARG_POLICIES: Dict[ArgPolicy, List[Optional[str]]] = \
{
    ArgPolicy.CONCAT: [
        "filter:v", "vf"
    ],
    ArgPolicy.MULTIPLE: [
        None, "i", "f", "map"
    ],
    ArgPolicy.FORBID: [
        "g"
    ]
}

# arguments that require quotes when getting the command
ARG_QUOTES: List[str] = ["filter:v", "vf", "i"]

# flags that don't take any arguments'
ARG_SINGLES: List[str] = ["y", "n", "v", "report", "vn", "an"]

@dataclass
class Param(object):
    spec: str
    value: str


def is_param_in_scope(p: Param, scope: ArgScope) -> bool:

    if scope == ArgScope.UNKNOWN:
        for specs in ARG_SCOPES.values():
            if p.spec in specs:
                return False
        return True

    return p.spec in ARG_SCOPES.get(scope)


class FFmpegCommand(object):


    params: List[Param]
    scope: ArgScope
    output: Optional[Param]


    def __init__(self, ffmpeg_bin: str, scope: ArgScope) -> None:

        self.params = []
        self.scope = ArgScope.UNKNOWN
        self.output = None

        self.params.append(
            Param(None, ffmpeg_bin)
        )
        self.scope = scope


    def validate(self) -> bool:

        # the minimum valid command line would be like:
        # ffmpeg -i input.mp4 output.mp4
        if len(self.params) < 3:
            return False

        # the last parameter is the output file
        if self.params[-1].spec is not None:
            return False

        _nospec_opt_cnt = 0
        for p in self.params:

            if not self.validate_param_scope(p):
                return False

            if p.spec is None:
                _nospec_opt_cnt += 1

        # allow only 2 arguments without specifiers: ffmpeg binary and output path
        return _nospec_opt_cnt == 2


    def validate_param_scope(self, p: Param) -> bool:

        if not (is_param_in_scope(p, self.scope) or is_param_in_scope(p, ArgScope.UNKNOWN)):
            return False
        return True


    def add_param(self, p: Param, skip_scope: bool=False) -> None:

        if not self.validate_param_scope(p) and not skip_scope:
            raise Exception("Invalid scope.")

        i = 0
        spec_encoutered_i = -1
        placeholders_idx = []

        while i < len(self.params):

            _p = self.params[i]

            if p.spec == _p.spec and spec_encoutered_i == -1:
                # found the same spec in command as current one
                spec_encoutered_i = i

            # TODO: add an option to skip placeholder replacement in templates
            if _p.value == "PLACEHOLDER":
                placeholders_idx.append(i)

            i += 1

        # restore i as this where the same spec value was seen
        i = spec_encoutered_i
        #print('encountered', p, i, placeholders_idx)

        # if the same spec was found
        if i != -1:

            # dealing with PLACEHOLDERs
            # iterate over found specs whose values are "PLACEHOLDER"
            for pidx in placeholders_idx:
                if p.spec == self.params[pidx].spec:
                    self.params[pidx].value = p.value
                    break
            else:
                # concat those
                if p.spec in ARG_POLICIES[ArgPolicy.CONCAT]:
                    self.params[i].value += f",{p.value}"
                # those can occur multiple times
                elif p.spec in ARG_POLICIES[ArgPolicy.MULTIPLE]:
                    self.params.append(p)
                # replace the value of those
                elif p.spec in ARG_POLICIES[ArgPolicy.FORBID]:
                    raise Exception("Param is not allowed.")
                else:
                    self.params[i].value = p.value

                if p.spec is None:
                    # there can only be one output
                    if self.output in self.params:
                        self.params.remove(self.output)
                    self.output = p
        else:
            # if spec doesn't exist in command line yet, just append it
            self.params.append(p)

        # keep output at the end
        if self.output is not None and self.params[-1] != self.output:
            self.params.remove(self.output)
            self.params.append(self.output)


    def get_command(self, without_bin: bool=False) -> str:

        cmd = ""

        for p in self.params[(1 if without_bin else 0):]:
            if p.spec is None:
                cmd += f'"{p.value}" '
            elif p.value is None:
                cmd += f"-{p.spec} "
            elif p.spec in ARG_QUOTES:
                cmd += f"-{p.spec} \"{p.value}\" "
            else:
                cmd += f"-{p.spec} {p.value} "

        return cmd



class Parser(object):

    scope: ArgScope
    cmds: List[FFmpegCommand]

    def __init__(self, scope: ArgScope) -> None:

        self.scope = scope
        self.cmds = [FFmpegCommand("ffmpeg", scope)]
        self._cmd_template = deepcopy(self.cmds[0])

    def parse_command(self, command_line: str, template: bool=False) -> None:

        ## posix=False means that for example the string:
        ## '-vf "scale=..."' will be interpreted as
        ## ["-vf", '"scale=..."'] preserving the quotes
        splitted_cmdline = shlex.split(command_line, posix=True)

        # distrffmpeg splits remote commands by using ffmpeg keyword
        # we need to know which one is the last remote command
        # because only from the last remote command are applied to everything
        last_remote_idx = splitted_cmdline[1:].count("ffmpeg")

        i = 1
        while i < len(splitted_cmdline):

            # TODO: check if it works properly
            is_last_remote = (last_remote_idx+1 == len(self.cmds))

            while i < len(splitted_cmdline):

                token = splitted_cmdline[i]

                if token.startswith("-") and not token == "-":

                    value = None
                    if token[1:] not in ARG_SINGLES:
                        value = splitted_cmdline[i+1]
                        i += 1

                    p = Param(token[1:], value)

                # start new command
                # only applies to remote scope
                elif token == "ffmpeg":

                    self.cmds.append(
                        deepcopy(self._cmd_template)
                    )

                    i += 1
                    break

                # treat any argument without dash at the beginning as output file
                else:
                    p = Param(None, token)

                if template:

                    self.cmds[-1].add_param(p, skip_scope=True)

                else:

                    if is_param_in_scope(p, self.scope):
                        self.cmds[-1].add_param(p)
                    elif is_param_in_scope(p, ArgScope.DISCARD):
                        pass
                    elif is_param_in_scope(p, ArgScope.UNKNOWN) and self.scope == ArgScope.REMOTE:
                        # treat any unclassified flag as remote flag
                        self.cmds[-1].add_param(p)

                i += 1

        # # this would be the final output file name
        # p = self.cmd_final.params[-1]
        # if p.spec == None:
        #     p.scope = ArgScope.FINAL

        if template:
            self._cmd_template = deepcopy(self.cmds[-1])

        #for cmd in self.cmds:
        #    print("cmd", cmd.get_command())

#if __name__ == '__main__':

#    CMDS = [
#        ('ffmpeg -y -i PLACEHOLDER -c copy -f segment -segment_frames 500 -segment_list /tmp/seg -reset_timestamps 1 -break_non_keyframes 1 "segs/out%06d.mkv"', ArgScope.PRE),
#        ('ffmpeg -y -f concat -safe 0 -i segments.txt -vf "select=between(n\,123\,234),setpts=N/FRAME_RATE/TB" -fps_mode passthrough -frame_pts true -an -g 10000 /tmp/0o34583458.mkv', ArgScope.REMOTE),
#        ('ffmpeg -y -f concat -safe 0 -i slices.txt -i PLACEHOLDER -c copy -map 0:v:0 -map 1:a:0 ', ArgScope.FINAL)
#    ]

#    for cmd, sc in CMDS:

#        print("cur scope", sc)
#        par = Parser(sc)

#        par.parse_command(cmd, template=True)
#        par.parse_command('ffmpeg -i hello.mp4 -vf "scale=1920x1080,vidstabdetect" -an -c:v libaom-av1 -crf 40 -pass 1 -f null - ffmpeg -i hello.mp4 -vf "scale=1920x1080,vidstabtransform" -c:v libaom-av1 -crf 40 -pass 2 -c:a libopus -b:a 32k helloout.mp4')

#        for cmd in par.cmds:
#            print(cmd.get_command(True))


#par.parse_command('ffmpeg -i hello.mp4 -vf "scale=1920x1080,vidstabdetect" -an -c:v libaom-av1 -crf 40 -pass 1 -f null - ffmpeg -i hello.mp4 -vf "scale=1920x1080,vidstabtransform" -c:v libaom-av1 -crf 40 -pass 2 -c:a libopus -b:a 32k helloout.mp4')

#par.parse_command('ffmpeg -i meme.mp4 -c:v libx265 -c:a libopus -b:a 32k -crf 25 -movflags +faststart out.mp4')

#par = Parser("ffmpeg -i input.mp4 output.mp4")

#print(par.get_cmd_remote())

"""
ff = FFmpegCommand("ffmpeg", ArgScope.REMOTE)
ff.add_param(Param("i", "video.mp4"))
ff.add_param(Param("vf", "scale=1920x1080"))
ff.add_param(Param(None, "output.mp4"))
ff.add_param(Param("vf", "vidstabdetect"))
print(ff.get_command())
print(ff.validate())"""
