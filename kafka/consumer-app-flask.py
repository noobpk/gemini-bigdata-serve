from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer

app = Flask(__name__, template_folder='templates', static_folder='templates/static')
app.config['SECRET_KEY'] = 'your_secret_key_here'
socketio = SocketIO(app)
consumer = KafkaConsumer(
            'gemini-data-streaming',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
        )

@app.route('/')
def index():
    return render_template('home/index.html')

@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

def kafka_message_consumer():
    for message in consumer:
            msg_key = message.key.decode('utf-8')
            msg_value = message.value.decode('utf-8')
            send_meg = {"topic": msg_key, "value": msg_value}
            socketio.emit('message', send_meg)

if __name__ == '__main__':
    socketio.start_background_task(kafka_message_consumer)
    socketio.run(app, debug=True)
