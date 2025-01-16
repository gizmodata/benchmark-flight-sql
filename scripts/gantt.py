import plotly.express as px
import pandas as pd
from pathlib import Path
import json
from munch import munchify, Munch

gantt_data = munchify(x={"Task": [], "Start": [], "Finish": []})
query_success_count: int = 0
query_failure_count: int = 0

for file_number in range(0, 10):
    print(f"File number: {file_number}")
    file_name = Path(f"data/benchmark-flight-sql-tpch-sf100-queries-{file_number}.json")

    with open(file=file_name, mode='r') as f:
        data = munchify(x=json.load(f))

        for query_run_result in data.query_run_results:
            query_success_count += query_run_result.success_count
            query_success_count += query_run_result.failure_count

            gantt_data.Task.append(f"{query_run_result.query.query_id} - pid: {file_number}")
            gantt_data.Start.append(query_run_result.runs[0].start_datetime)
            gantt_data.Finish.append(query_run_result.runs[0].end_datetime)

print(f"Total query success count: {query_success_count}")
print(f"Total query failure count: {query_failure_count}")

df = pd.DataFrame(gantt_data).sort_values(by="Start", ascending=False)

# Create the Gantt chart
fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task")

# Customize the plot
fig.update_layout(title='Gantt Chart', xaxis_title='Time', yaxis_title='Tasks')

# Show the plot
fig.show()
