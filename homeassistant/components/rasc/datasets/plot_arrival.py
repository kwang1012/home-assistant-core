import pandas as pd
import matplotlib.pyplot as plt

# Load the first dataset from a CSV file
file_path1 = 'arrival_all.csv'  # Replace with your first file path
data1 = pd.read_csv(file_path1, names=['timestamp', 'routineID', 'routineName'])

# Load the second dataset from a CSV file
file_path2 = 'arrival_all_hybrid.csv'  # Replace with your second file path
data2 = pd.read_csv(file_path2)

# Calculate cumulative arrival times for both datasets
data1['cumulative_arrival_time'] = data1['timestamp'].cumsum()
data2['cumulative_arrival_time'] = data2['timestamp'].cumsum()

# Add a sequential number for each routine
data1['routine_number'] = range(1, len(data1) + 1)
data2['routine_number'] = range(1, len(data2) + 1)

# Create the plot with figsize=(6, 4)
plt.figure(figsize=(6, 4))

# Plot the first dataset with a custom color and marker
plt.plot(data1['cumulative_arrival_time'], data1['routine_number'],
         marker='o', linestyle='-', color='blue', markerfacecolor='red', label='all')

# Plot the second dataset with a different color and marker
plt.plot(data2['cumulative_arrival_time'], data2['routine_number'],
         marker='s', linestyle='--', color='green', markerfacecolor='yellow', label='all_hybrid')

# Label the axes with increased font size
plt.xlabel('Arrival Time (s)', fontsize=20)
plt.ylabel('Routine ID', fontsize=20)

# Set title with increased font size
# plt.title('Routine Arrival Time vs. Routine Sequence', fontsize=14)

# Increase font size for tick labels
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)

# Add a legend with font size 14
plt.legend(fontsize=20)

# Show the plot
plt.show()

plt.savefig('arrival_times_plot.pdf', bbox_inches='tight')
