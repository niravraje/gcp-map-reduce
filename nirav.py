import socket
import subprocess

print("host", socket.gethostbyname(socket.gethostname()))

code = subprocess.call(["/bin/bash", "./shell-scripts/nirav.sh", "yoyo", "123"])
