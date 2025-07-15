import subprocess
import time
import requests
import json

# Настройки подключения
ssh_user = "maximus"
ssh_host = "192.168.1.88"
ssh_password = "31313233WWW3"
local_port = 11434

# Имя модели, которую запущена на Windows через Ollama
model_name = "gemma3n"  # замените на нужную (например, "gemma3n")

# Prompt для модели
prompt = "Ты слышишь меня?"

# Устанавливаем SSH-туннель с помощью sshpass
def create_ssh_tunnel():
    cmd = [
        "sshpass", "-p", ssh_password,
        "ssh", "-f", "-N",
        f"-L{local_port}:localhost:{local_port}",
        f"{ssh_user}@{ssh_host}"
    ]
    try:
        subprocess.run(cmd, check=True)
        print("[✓] SSH-туннель установлен.")
    except subprocess.CalledProcessError as e:
        print("[!] Ошибка при создании SSH-туннеля:", e)

# Отправляем запрос к Ollama
def query_ollama():
    url = f"http://localhost:{local_port}/api/generate"
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": model_name,
        "prompt": prompt,
        "stream": False
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        data = response.json()
        print("Ответ от модели:")
        print(data.get("response", data))
    except requests.RequestException as e:
        print("[!] Ошибка при обращении к API:", e)

# --- Выполнение ---
if __name__ == "__main__":
    print("[…] Создаём SSH-туннель...")
    create_ssh_tunnel()
    time.sleep(1)  # Небольшая пауза для стабильности
    print("[…] Отправляем запрос к Ollama...")
    query_ollama()
