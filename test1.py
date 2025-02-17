from flask import Flask, request
import socket

app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello, world! running on %s' % request.host

if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('10.100.136.47', 0))
    port = sock.getsockname()[1]
    sock.close()
    app.run(host="10.100.136.47",port=port)
