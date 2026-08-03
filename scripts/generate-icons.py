#!/usr/bin/env python3
"""Regenerate packaging/icons/* and macOS AppIcon.icns from a single drawing.

Requires: Pillow (stdlib only for .icns pack).
"""
from __future__ import annotations

import struct
from pathlib import Path

try:
    from PIL import Image, ImageDraw
except ImportError as e:
    raise SystemExit("Pillow required: python3 -m pip install Pillow") from e

ROOT = Path(__file__).resolve().parents[1]
ICON_DIR = ROOT / "packaging" / "icons"
MACOS_DIR = ROOT / "packaging" / "macos"


def make_icon(size: int) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    m = max(1, size // 16)
    r = size // 5
    d.rounded_rectangle(
        [m, m, size - m - 1, size - m - 1],
        radius=r,
        fill=(14, 18, 24, 255),
        outline=(30, 120, 200, 255),
        width=max(1, size // 32),
    )
    cx = cy = size // 2
    rad = size // 4
    d.ellipse([cx - rad, cy - rad, cx + rad, cy + rad], fill=(30, 120, 200, 255))
    ir = rad * 2 // 3
    d.ellipse([cx - ir, cy - ir, cx + ir, cy + ir], fill=(80, 200, 255, 255))
    s = size
    bolt = [
        (cx - s // 12, cy - s // 5),
        (cx + s // 20, cy - s // 5),
        (cx - s // 40, cy - s // 40),
        (cx + s // 10, cy - s // 40),
        (cx - s // 20, cy + s // 5),
        (cx + s // 40, cy + s // 40),
        (cx - s // 12, cy + s // 40),
    ]
    d.polygon(bolt, fill=(255, 255, 255, 255))
    return img


def write_icns(png_by_size: dict[int, Path], out: Path) -> None:
    type_map = {
        16: b"icp4",
        32: b"icp5",
        64: b"icp6",
        128: b"ic07",
        256: b"ic08",
        512: b"ic09",
        1024: b"ic10",
    }
    chunks: list[bytes] = []
    for size, path in sorted(png_by_size.items()):
        t = type_map.get(size)
        if not t:
            continue
        data = path.read_bytes()
        chunks.append(t + struct.pack(">I", 8 + len(data)) + data)
    body = b"".join(chunks)
    total = 8 + len(body)
    out.write_bytes(b"icns" + struct.pack(">I", total) + body)


def main() -> None:
    ICON_DIR.mkdir(parents=True, exist_ok=True)
    MACOS_DIR.mkdir(parents=True, exist_ok=True)
    master = make_icon(1024)
    master.save(ICON_DIR / "joule-1024.png")
    pngs: dict[int, Path] = {1024: ICON_DIR / "joule-1024.png"}
    for s in (512, 256, 128, 64, 48, 32, 16):
        p = ICON_DIR / f"joule-{s}.png"
        master.resize((s, s), Image.Resampling.LANCZOS).save(p)
        pngs[s] = p
    master.resize((256, 256), Image.Resampling.LANCZOS).save(ICON_DIR / "joule.png")
    icos = [make_icon(s) for s in (256, 128, 64, 48, 32, 16)]
    icos[0].save(
        ICON_DIR / "joule.ico",
        format="ICO",
        sizes=[(i.width, i.height) for i in icos],
        append_images=icos[1:],
    )
    write_icns({s: pngs[s] for s in (16, 32, 64, 128, 256, 512, 1024)}, MACOS_DIR / "AppIcon.icns")
    print("icons:", sorted(p.name for p in ICON_DIR.iterdir()))
    print("icns:", MACOS_DIR / "AppIcon.icns")


if __name__ == "__main__":
    main()
