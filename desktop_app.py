import threading
import time
import socket
import sys
import uvicorn

def _start_server():
    # Запускаем FastAPI через uvicorn
    uvicorn.run("backend.main:app", host="127.0.0.1", port=8000, log_level="info")

def _wait_for_port(host: str, port: int, timeout: int = 10) -> bool:
    """Ждём, пока указанный порт станет доступным."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except Exception:
            time.sleep(0.5)
    return False

def main():
    # Фоновый поток для uvicorn
    thread = threading.Thread(target=_start_server, daemon=True)
    thread.start()
    if not _wait_for_port("127.0.0.1", 8000):
        print("❌ Не удалось подключиться к серверу 127.0.0.1:8000")
        sys.exit(1)
    # Попытка импортировать pywebview
    try:
        import webview
    except ImportError:
        print("❌ pywebview не установлен. Установите его: pip install pywebview")
        sys.exit(1)
    # Создаём окно
    webview.create_window("Risk Process Orchestrator MVP", "http://127.0.0.1:8000", width=1360, height=860)
    webview.start()


if __name__ == "__main__":
    main()
