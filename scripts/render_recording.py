"""Render an actual asciinema v2 recording to a readable H.264 terminal video.

The event timing and output come from the recorded process. pyte emulates its
terminal; Pillow draws the terminal cells; FFmpeg encodes the resulting frames.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
from pathlib import Path

import pyte
from PIL import Image, ImageDraw, ImageFont


NOTES = {
    0: ("项目与调用链", "应用调用统一 API；网关负责鉴权、额度、路由和用量结算。"),
    1: ("01 / 普通响应", "读取真实模型回答与 usage；等待请求结算后再核对 Token 回执。"),
    2: ("02 / SSE 流式响应", "保留本次生成的逐步输出时间；最终用量仍按上游 usage 结算。"),
    3: ("03 / 幂等重放", "重复提交相同请求：request_id 一致，已用 Token 增量为 0。"),
    4: ("04 / 额度控制", "账户可用额度耗尽后，网关返回 HTTP 402 / quota_exceeded。"),
    5: ("05 / Key 管理", "撤销临时 API Key 后，携带该 Key 的访问返回 HTTP 401。"),
    6: ("本次运行结果", "五步真实 API 检查；核对账户用量与请求回执，并清理临时凭证。"),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--font-cjk", type=Path, required=True)
    parser.add_argument("--font-mono", type=Path, default=Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"))
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--end-hold", type=float, default=12, help="Seconds to hold the final result page")
    args = parser.parse_args()
    with args.recording.open() as source:
        header = json.loads(next(source))
        events = [json.loads(line) for line in source if line.strip()]
    if header.get("version") != 2 or not events:
        raise ValueError("A nonempty asciinema v2 recording is required")
    if not 1 <= args.fps <= 30:
        raise ValueError("fps must be between 1 and 30")
    duration = float(events[-1][0])+args.end_hold
    screen = pyte.Screen(header["width"], header["height"])
    stream = pyte.Stream(screen)
    latin = ImageFont.truetype(str(args.font_mono), 24)
    cjk = ImageFont.truetype(str(args.font_cjk), 25)
    title = ImageFont.truetype(str(args.font_mono), 41)
    label = ImageFont.truetype(str(args.font_cjk), 24)
    small = ImageFont.truetype(str(args.font_cjk), 20)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    poster = args.output.with_suffix(".jpg")
    manifest = args.output.with_suffix(".json")
    chapter_times = {0: 0.0}

    def compose(phase):
        frame = Image.new("RGB", (1920, 1080), "#0b1020")
        draw = ImageDraw.Draw(frame)
        draw.text((60, 29), "LLM Gateway", font=title, fill="#f0f5ff")
        draw.text((60, 84), "Go 大模型网关与用量管理  /  真实终端录制", font=label, fill="#aebcda")
        draw.rounded_rectangle((1400, 47, 1858, 105), radius=14, fill="#123b3d")
        draw.text((1422, 61), "本机 Qwen3-VL-8B · API 实测", font=small, fill="#8df1d3")
        draw.rounded_rectangle((52, 145, 1868, 949), radius=18, fill="#101827", outline="#293954", width=2)
        draw.rounded_rectangle((53, 146, 1867, 200), radius=17, fill="#1d293d")
        for x, color in ((80, "#fa706c"), (105, "#e6bd58"), (130, "#63c695")):
            draw.ellipse((x, 165, x+13, 178), fill=color)
        draw.text((170, 157), "bash run.sh showcase --presentation", font=small, fill="#ced8ef")
        for row in range(screen.lines):
            for column in range(screen.columns):
                cell = screen.buffer[row][column]
                if not cell.data or cell.data == " ":
                    continue
                color = "#e2e9f6"
                if row == 0:
                    color = "#81dfcb"
                elif row == 1:
                    color = "#ffd288"
                draw.text((83+column*15.7, 218+row*32), cell.data,
                          font=latin if all(ord(c) < 128 for c in cell.data) else cjk,
                          fill=color)
        heading, note = NOTES[phase]
        draw.text((63, 964), heading, font=label, fill="#81dfcb")
        draw.text((390, 967), note, font=small, fill="#c4cfe2")
        draw.text((63, 1015), "yeruoxiangchen / llm_gateway", font=small, fill="#8a9ab6")
        draw.text((1390, 1015), "asciinema → pyte → FFmpeg", font=small, fill="#8a9ab6")
        return frame

    frames = math.ceil(duration*args.fps)+1
    command = [args.ffmpeg, "-hide_banner", "-loglevel", "warning", "-n",
               "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", "1920x1080", "-r", str(args.fps), "-i", "pipe:0",
               "-an", "-c:v", "libx264", "-threads", "2", "-preset", "fast", "-crf", "21",
               "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(args.output)]
    phase = 0
    next_event = 0
    raw = None
    poster_saved = False
    with args.output.with_suffix(".ffmpeg.log").open("w") as log:
        encoder = subprocess.Popen(command, stdin=subprocess.PIPE, stderr=log)
        try:
            for index in range(frames):
                at = index/args.fps
                dirty = raw is None
                while next_event < len(events) and float(events[next_event][0]) <= at:
                    timestamp, kind, data = events[next_event]
                    next_event += 1
                    if kind != "o":
                        continue
                    match = re.search(r"([1-5])/5 ", data)
                    if match:
                        phase = int(match[1])
                        chapter_times.setdefault(phase, float(timestamp))
                    if "运行结果 / 本次真实 API 演示" in data:
                        phase = 6
                        chapter_times.setdefault(phase, float(timestamp))
                    stream.feed(data)
                    dirty = True
                if dirty:
                    frame = compose(phase)
                    raw = frame.tobytes()
                    if phase == 3 and '"added_used_tokens": 0' in "\n".join(screen.display):
                        frame.save(poster, quality=91)
                        poster_saved = True
                encoder.stdin.write(raw)
                if index % (args.fps*20) == 0:
                    print(json.dumps({"rendered_seconds": round(at, 1), "duration_seconds": round(duration, 2)}), flush=True)
            encoder.stdin.close()
            if encoder.wait() != 0:
                raise RuntimeError(f"FFmpeg failed; see {log.name}")
        except BaseException:
            if encoder.poll() is None:
                encoder.terminate()
                encoder.wait()
            raise
    if not poster_saved:
        compose(phase).save(poster, quality=91)
    manifest.write_text(json.dumps({"recording": args.recording.name, "video": args.output.name,
                                   "duration_seconds": round(frames/args.fps, 3), "fps": args.fps,
                                   "resolution": [1920, 1080], "audio": False,
                                   "timing": "original asciinema output timestamps, followed by a final still hold",
                                   "final_hold_seconds": args.end_hold,
                                   "chapters": [{"at_seconds": round(at, 3), "title": NOTES[p][0]}
                                                for p, at in chapter_times.items()],
                                   "video_bytes": args.output.stat().st_size}, ensure_ascii=False, indent=2)+"\n")
    print(manifest.read_text(), flush=True)


if __name__ == "__main__":
    main()
