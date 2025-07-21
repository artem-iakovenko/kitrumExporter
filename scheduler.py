import subprocess
import time

while True:
    subprocess.run(["python", "sync_launcher.py"])
    print("\nCompleted! 24h pause")
    time.sleep(60*24)
