#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


PROFILE_BLOCK_START = "# BEGIN polymarket-bot profiles"
PROFILE_BLOCK_END = "# END polymarket-bot profiles"

PROFILE_BLOCK = f"""{PROFILE_BLOCK_START}
[profiles.polymarket-research]
model = "gpt-5.4"
model_reasoning_effort = "xhigh"
approval_policy = "never"
sandbox_mode = "workspace-write"
search = true

[profiles.polymarket-controller]
model = "gpt-5.4"
model_reasoning_effort = "xhigh"
approval_policy = "never"
sandbox_mode = "workspace-write"
search = true

[profiles.polymarket-build]
model = "gpt-5.4"
model_reasoning_effort = "xhigh"
approval_policy = "never"
sandbox_mode = "workspace-write"
search = true
{PROFILE_BLOCK_END}
"""


def install_profiles(config_path: Path) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    existing = config_path.read_text() if config_path.exists() else ""

    if PROFILE_BLOCK_START in existing and PROFILE_BLOCK_END in existing:
        before, remainder = existing.split(PROFILE_BLOCK_START, 1)
        _, after = remainder.split(PROFILE_BLOCK_END, 1)
        new_text = before.rstrip() + "\n\n" + PROFILE_BLOCK.strip() + "\n" + after.lstrip()
    else:
        new_text = existing.rstrip()
        if new_text:
            new_text += "\n\n"
        new_text += PROFILE_BLOCK.strip() + "\n"

    config_path.write_text(new_text)


def install_skills(repo_root: Path, codex_home: Path) -> None:
    source_root = repo_root / "job-orc" / "skills-src"
    target_root = codex_home / "skills"
    target_root.mkdir(parents=True, exist_ok=True)

    for skill_dir in sorted(source_root.iterdir()):
        if not skill_dir.is_dir():
            continue
        destination = target_root / skill_dir.name
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(skill_dir, destination)


def verify_codex() -> None:
    result = subprocess.run(["codex", "--version"], capture_output=True, text=True, check=True)
    print(result.stdout.strip())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--codex-home", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()

    codex_home = args.codex_home
    config_path = args.config or (codex_home / "config.toml")

    verify_codex()
    install_profiles(config_path)
    install_skills(args.repo_root, codex_home)

    print(f"Configured Codex profiles in {config_path}")
    print(f"Installed project skills into {codex_home / 'skills'}")


if __name__ == "__main__":
    main()
