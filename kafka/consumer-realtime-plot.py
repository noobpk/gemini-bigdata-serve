from kafka import KafkaConsumer
import pandas as pd
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib.dates import DateFormatter

consumer = KafkaConsumer("gemini-data-streaming", bootstrap_servers=["localhost:9092"])

save_data = []  # for save
temp_data = []  # for plot

# Create an empty plot
fig, ax = plt.subplots()
(line,) = ax.plot([], [], label="Score")
ax.set_xlabel("Time")
ax.set_ylabel("Score")
ax.set_title("Real-Time Predict Request/Response")
ax.legend()

annotations = []


def update_plot(frame):
    key = frame.key.decode("utf-8") if frame.key else None
    value = frame.value.decode("utf-8") if frame.value else None
    print(
        "%s:%d:%d: key=%s value=%s"
        % (frame.topic, frame.partition, frame.offset, frame.key, frame.value)
    )

    if key == "threat_metrix":
        new_data = json.loads(value)
        save_data.append(new_data)
        temp_data.append(new_data)

        df = pd.DataFrame(save_data)
        df.to_csv("gemini_realtime_predict_req_resp.csv")

        # Keep only the most recent data, removing data at position 0
        max_data_points = 5  # Adjust this value as needed
        if len(temp_data) > max_data_points:
            temp_data.pop(0)

        df_temp = pd.DataFrame(temp_data)
        df_temp["time"] = pd.to_datetime(
            df_temp["time"]
        )  # Convert 'time' column to datetime

        line.set_data(df_temp["time"], df_temp["score"])  # Set data for the line plot
        ax.relim()
        ax.autoscale_view()
        ax.xaxis.set_major_formatter(DateFormatter("%H:%M:%S"))

        for annotation in annotations:
            annotation.remove()  # Remove previous annotations
        annotations.clear()  # Clear the list
        for idx, row in df_temp.iterrows():
            if row["rbd_xss"]:
                xss_color = "red"
            else:
                xss_color = "black"

            if row["rbd_sqli"]:
                sqli_color = "red"
            else:
                sqli_color = "black"

            if row["rbd_unknown"]:
                unknow_color = "red"
            else:
                unknow_color = "black"

            annotation_1 = ax.annotate(
                f"IP: {row['ipaddress']}",
                (row["time"], row["score"]),
                textcoords="offset points",
                xytext=(80, 20),
                ha="center",
            )
            annotation_2 = ax.annotate(
                f"XSS: {row['rbd_xss']}",
                (row["time"], row["score"]),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                color=xss_color,
            )
            annotation_3 = ax.annotate(
                f"SQLi: {row['rbd_sqli']}",
                (row["time"], row["score"]),
                textcoords="offset points",
                xytext=(60, 10),
                ha="center",
                color=sqli_color,
            )
            annotation_4 = ax.annotate(
                f"UNKNOW: {row['rbd_unknown']}",
                (row["time"], row["score"]),
                textcoords="offset points",
                xytext=(130, 10),
                ha="center",
                color=unknow_color,
            )
            annotations.append(annotation_1)
            annotations.append(annotation_2)
            annotations.append(annotation_3)
            annotations.append(annotation_4)


ani = FuncAnimation(
    fig, update_plot, consumer, interval=1000
)  # Update plot every 1 second

plt.tight_layout()
plt.show()
