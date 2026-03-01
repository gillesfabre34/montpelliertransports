# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from consumer.spark_stream import read_spark
from producer.gtfs_producer import run_producer

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('Logos')
    run_producer()
    read_spark()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
