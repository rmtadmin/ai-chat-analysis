#!/usr/bin/env python3
"""
client_chat_processor.py — v0.4.1 (zero‑arg, bug‑fix)
====================================================
* Полностью завершается `print` в конце, добавлен блок `except` и guard
  `if __name__ == "__main__": main()`.
* Исправлена случайная усечённая строка, из‑за которой возникал `SyntaxError`.
"""
from __future__ import annotations
import re

from db import insert_json_result
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
import requests
import hashlib

def compute_text_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

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
    # 🧠 Робастный парсинг
    if isinstance(result, dict):
        parsed = result
    else:
        # Попытка извлечь JSON-блок из строки
        match = re.search(r"\{.*\}", result, re.DOTALL)
        if not match:
            raise ValueError("Не удалось найти JSON-объект в ответе LLM")
        cleaned = match.group(0)
        parsed = json.loads(cleaned)

    return {"response": result, "parsed": parsed}



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
    sys_msg = (
        "Ты — аналитический ассистент. Из переписки клиента определи:"\
        "\n1. Есть ли заказ и его параметры (материал, размеры, цвет)."\
        "\n2. Цену, если она фигурирует."\
        "\n3. Жалобы/рекламации."\
        "\n\nВерни результат в JSON со схемой: "\
        "{'has_order': bool, 'order': {...}|null, 'complaint': str|null, 'summary': str}.\n"
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




def save_result(output_dir: Path, name: str, response: str, host: str, model: str):
    json_part = extract_json_from_response(response)
    parsed = fix_keys(json.loads(json_part))

    out_path = output_dir / f"{name}_analysis.json"
    out_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path, parsed

def extract_json_from_response(text: str) -> str:
    if "```json" in text:
        text = text.split("```json", 1)[-1]
    if "```" in text:
        text = text.split("```", 1)[0]
    return text.strip()

def fix_keys(d: dict) -> dict:
    """Исправляет опечатки в ключах LLM-ответа."""
    replacements = {
        "surnary": "summary",
        "parametrs": "parameters",
    }
    return {
        replacements.get(k, k): v if not isinstance(v, dict) else fix_keys(v)
        for k, v in d.items()
    }


def slugify(value) -> str:
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r"[^\w]+", "_", value.lower()).strip("_")



def load_all_chats(path: Path) -> list[tuple[str, list[str]]]:
    raw = json.loads(path.read_text(encoding="utf-8"))

    # 💡 поддержа Telegram-архива
    if isinstance(raw, dict) and "chats" in raw and "list" in raw["chats"]:
        raw = raw["chats"]["list"]

    if not (isinstance(raw, list) and all("messages" in d for d in raw)):
        raise ValueError("Файл не содержит список чатов с ключом 'messages'")

    chats = []
    for i, entry in enumerate(raw):
        name = entry.get("name") or f"chat_{i+1}"
        msgs = []
        for msg in entry["messages"]:
            if msg.get("type") == "message":
                txt = msg.get("text")
                if isinstance(txt, str):
                    msgs.append(txt)
                elif isinstance(txt, list):
                    flat = "".join(part if isinstance(part, str) else part.get("text", "") for part in txt)
                    msgs.append(flat)
        chats.append((name, msgs))
    return chats

def robust_json_parse(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise TypeError("Ожидалась строка или словарь")

    match = re.search(r"{.*}", value, re.DOTALL)
    if not match:
        raise ValueError("Не удалось найти JSON-объект в строке")

    return json.loads(match.group(0))

def append_history(entry: dict, path: Path = Path("output/history.jsonl")) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def load_history_hashes(path: Path = Path("output/history.jsonl")) -> set[str]:
    if not path.exists():
        return set()
    hashes = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            entry = json.loads(line)
            if "chat_hash" in entry:
                hashes.add(entry["chat_hash"])
        except Exception:
            continue
    return hashes


# ────────────────────────────────────────────────────────────────────────────────
# 🚀 Main
# ────────────────────────────────────────────────────────────────────────────────

def main() -> None:
    done_hashes = load_history_hashes   ()
    cfg = EnvConfig()

    if not cfg.chat_file.exists():
        print("❌ Укажите существующий файл в CHAT_FILE", file=sys.stderr)
        sys.exit(1)

    if cfg.llm_host:
        host_cm = contextmanager(lambda: (yield cfg.llm_host))()
    else:
        if not cfg.ssh_host:
            print("❌ Не указан ни LLM_HOST, ни SSH_HOST", file=sys.stderr)
            sys.exit(1)
        host_cm = SshTunnel(cfg)

    with host_cm as host_url:
        try:
            all_chats = load_all_chats(cfg.chat_file)
            for idx, (chat_name, messages) in enumerate(all_chats, 1):
                try:
                    chat_text = "\n".join(messages)
                    chat_hash = compute_text_hash(chat_text)

                    if chat_hash in done_hashes:
                        print(f"⏭️  Пропускаем (не изменился): {chat_name}")
                        continue

                    prompt = prepare_prompt(chat_text)
                    llm_data = call_llm(prompt, host_url, cfg.llm_model)
                    parsed = robust_json_parse(llm_data["response"])

                    filename_stub = f"{idx}_{slugify(chat_name)}"
                    out, _ = save_result(cfg.output_dir, filename_stub, llm_data["response"], host_url, cfg.llm_model)

                    result_data = {
                        "id": str(uuid.uuid4()),
                        "source_file": str(cfg.chat_file),
                        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "host": host_url,
                        "model": cfg.llm_model,
                        "result": parsed,
                        "success": True,
                        "error": None,
                        "chat_hash": chat_hash,
                    }
                    insert_json_result(result_data)

                    append_history({
                        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "chat_file": str(cfg.chat_file),
                        "output": str(out),
                        "model": cfg.llm_model,
                        "host": host_url,
                        "success": True,
                        "chat_hash": chat_hash,
                    })
                    print(f"✅ Сохранено: {out.name}")
                except Exception as e:
                    print(f"❌ Ошибка чата {chat_name}: {e}")
                    error_data = {
                        "id": str(uuid.uuid4()),
                        "source_file": str(cfg.chat_file),
                        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "host": host_url,
                        "model": cfg.llm_model,
                        "result": {},
                        "success": False,
                        "error": str(e),
                        "chat_hash": chat_hash,
                    }
                    insert_json_result(error_data)

        except Exception as e:
            print(f"❌ Общая ошибка: {e}", file=sys.stderr)
            sys.exit(1)



if __name__ == "__main__":
    main()

