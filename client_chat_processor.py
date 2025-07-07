#!/usr/bin/env python3
"""
client_chat_processor.py — v0.4.1 (zero‑arg, bug‑fix)
====================================================
* Полностью завершается `print` в конце, добавлен блок `except` и guard
  `if __name__ == "__main__": main()`.
* Исправлена случайная усечённая строка, из‑за которой возникал `SyntaxError`.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests

# ────────────────────────────────────────────────────────────────────────────────
# 🔧 Конфигурация
# ────────────────────────────────────────────────────────────────────────────────
@dataclass
class EnvConfig:
    """Читаем все настройки из env‑переменных."""

    chat_file: Path = Path(os.getenv("CHAT_FILE", "data/chat.json"))
    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "output"))

    # LLM connection (direct or via SSH)
    llm_host: Optional[str] = os.getenv("LLM_HOST")
    llm_model: str = os.getenv("LLM_MODEL", "gemma3n")
    api_key: Optional[str] = os.getenv("LLM_API_KEY")

    # SSH tunnel settings (used when llm_host is None)
    ssh_host: Optional[str] = os.getenv("SSH_HOST")
    ssh_user: str = os.getenv("SSH_USER", os.getenv("USER", ""))
    ssh_port: int = int(os.getenv("SSH_PORT", 22))
    remote_port: int = int(os.getenv("REMOTE_PORT", 11434))
    local_port: int = int(os.getenv("LOCAL_PORT", 11434))
    ssh_pass: Optional[str] = os.getenv("SSH_PASS")


# ────────────────────────────────────────────────────────────────────────────────
# 🔌 SSH‑туннель
# ────────────────────────────────────────────────────────────────────────────────
class SshTunnel:
    def __init__(self, cfg: EnvConfig):
        self.cfg = cfg
        self.proc: subprocess.Popen | None = None

    def __enter__(self) -> str:  # noqa: D401
        target = f"{self.cfg.ssh_user}@{self.cfg.ssh_host}"
        cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-p",
            str(self.cfg.ssh_port),
            "-L",
            f"{self.cfg.local_port}:localhost:{self.cfg.remote_port}",
            target,
            "-N",
        ]
        if self.cfg.ssh_pass:
            cmd = ["sshpass", "-p", self.cfg.ssh_pass] + cmd
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid if os.name != "nt" else None,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )
        time.sleep(1)  # даём туннелю подняться
        return f"http://localhost:{self.cfg.local_port}"

    def __exit__(self, exc_type, exc, tb):  # noqa: D401
        if self.proc and self.proc.poll() is None:
            try:
                if os.name == "nt":
                    self.proc.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    os.killpg(self.proc.pid, signal.SIGTERM)
            except Exception:  # pylint: disable=broad-except
                pass
            self.proc.wait(timeout=5)


# ────────────────────────────────────────────────────────────────────────────────
# 🧩 Вызов LLM
# ────────────────────────────────────────────────────────────────────────────────

def call_llm(prompt: str, host: str, model: str, api_key: str | None = None) -> Dict[str, Any]:
    url = f"{host.rstrip('/')}/api/generate"
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"    
    payload = {"model": model, "prompt": prompt}

    try:
        resp = requests.post(url, json=payload, headers=headers, stream=True, timeout=180)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"LLM request failed: {e}") from e

    chunks = []
    print("📡 Поток данных от LLM:")
    for line in resp.iter_lines(decode_unicode=True):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            chunk = obj.get("response", "")
            chunks.append(chunk)
            print("🧩", repr(chunk))
        except json.JSONDecodeError as e:
            print("❌ Ошибка парсинга NDJSON строки:", line)
            raise

    result = "".join(chunks).strip()
    return {"response": result}



# ────────────────────────────────────────────────────────────────────────────────
# 📝 Prompt
# ────────────────────────────────────────────────────────────────────────────────

def prepare_prompt(chat_text: str, max_chars: int = 15_000) -> str:
    sys_msg = (
        "Ты — аналитик клиентской переписки.\n"
        "На входе тебе даётся диалог (формат Telegram).\n"
        "Твоя задача — извлечь из него структурированную информацию:\n\n"
        "- был ли заказ (`has_order`)\n"
        "- параметры изделий (материал, цвет, размер, стоимость, количество)\n"
        "- наличие жалоб или рекламаций (`complaint`)\n"
        "- итоговая сумма заказа (`total_sum`)\n"
        "- краткое резюме чата (`summary`)\n\n"
        "Если информация отсутствует, явно укажи это (например, `has_order: false`).\n"
        "Ответ верни строго в формате JSON.\n\n"
        "Вводной диалог находится между тегами <chat> ... </chat>"
    )
    return (f"<system>\n{sys_msg}\n</system>\n<chat>\n{chat_text}\n</chat>")[:max_chars]



# ────────────────────────────────────────────────────────────────────────────────
# 📂 I/O helpers
# ────────────────────────────────────────────────────────────────────────────────

def load_chat(path: Path) -> str:
    print(f"📄 Загружаем файл: {path}")
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")
    
    if path.suffix.lower() == ".json":
        print(f"🔍 Определён формат: JSON")

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print("❌ Ошибка при загрузке JSON:", e)
            raise

        print(f"📦 Тип корневого объекта: {type(data)}")
        if isinstance(data, dict):
            if "messages" in data:
                print("✅ Найден ключ 'messages' — Telegram формат")
                data = data["messages"]
            else:
                raise ValueError("❌ Ожидался массив сообщений или ключ 'messages' в JSON")
        elif isinstance(data, list):
            print("✅ JSON — это список")
        else:
            raise TypeError("❌ Неизвестная структура JSON-файла")

        texts = []
        for msg in data:
            if not isinstance(msg, dict):
                continue
            text = msg.get("text", "")
            if isinstance(text, list):
                text = "".join(part.get("text", "") for part in text if isinstance(part, dict))
            if isinstance(text, str) and text.strip():
                texts.append(text.strip())

        print(f"🧾 Извлечено сообщений: {len(texts)}")
        return "\n".join(texts)

    print(f"📄 Чтение как обычный текстовый файл")
    return path.read_text(encoding="utf-8")



def save_result(out_dir: Path, src_file: Path, llm_resp: Dict[str, Any], host: str, model: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{src_file.stem}_analysis.json"
    result = {
        "id": str(uuid.uuid4()),
        "source_file": str(src_file),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "host": host,
        "model": model,
        "llm_response": llm_resp["response"],
    }
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


# ────────────────────────────────────────────────────────────────────────────────
# 🚀 Main
# ────────────────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = EnvConfig()

    # autodiscover chat file if default not present
    if not cfg.chat_file.exists():
        candidates = list(Path("data").glob("*.json")) + list(Path("data").glob("*.txt"))
        if not candidates:
            print("❌ No chat file found (set CHAT_FILE)", file=sys.stderr)
            sys.exit(1)
        cfg.chat_file = candidates[0]
        print(f"ℹ️  Using first chat file: {cfg.chat_file}")

    # Determine connection
    if cfg.llm_host:
        host_cm = contextmanager(lambda: (yield cfg.llm_host))()
    else:
        if not cfg.ssh_host:
            print("❌ Set LLM_HOST or SSH_HOST env variable", file=sys.stderr)
            sys.exit(1)
        host_cm = SshTunnel(cfg)

    with host_cm as host_url:
        try:
            chat_text = load_chat(cfg.chat_file)
            prompt = prepare_prompt(chat_text)
            llm_data = call_llm(prompt, host_url, cfg.llm_model, cfg.api_key)
            out = save_result(cfg.output_dir, cfg.chat_file, llm_data, host_url, cfg.llm_model)
            append_history({
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "chat_file": str(cfg.chat_file),
                "output": str(out),
                "model": cfg.llm_model,
                "host": host_url,
                "success": True,
            })
            try:
                print(f"✅ Saved → {out.relative_to(Path.cwd())}")
            except ValueError:
                print(f"✅ Saved → {out}")

        except Exception as exc:  # pylint: disable=broad-except
            print(f"❌ Error: {exc}", file=sys.stderr)
            append_history({
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "chat_file": str(cfg.chat_file),
                "model": cfg.llm_model,
                "host": cfg.llm_host or cfg.ssh_host,
                "success": False,
                "error": str(exc),
            })
            sys.exit(1)

def append_history(entry: dict, path: Path = Path("output/history.jsonl")) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
