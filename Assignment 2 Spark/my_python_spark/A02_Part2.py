# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark

import calendar
import datetime

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def current_date(line):
    # 1. We create the output variable
    res = ()
    fmt = "%d-%m-%Y %H:%M:%S"
    day = calendar.day_name[(datetime.datetime.strptime(line[4], fmt)).weekday()]
    time = datetime.datetime.strptime(line[4], fmt).hour
    res = (str(day)+"_"+str(time), 1)
    return res

def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name):
    inputs = sc.textFile(my_dataset_dir)
    my_tuple = inputs.map(process_line)
    filters = my_tuple.filter(lambda line: line[0] == "0" and line[5] == "0" and line[1] == station_name)
    filters.persist()

    score = filters.map(lambda line: (line[1], 1))
    reduced = score.aggregateByKey(0, lambda a, b: a + b, lambda a, b: a + b)
    total = reduced.map(lambda a: a[1])
    fullTotal = total.collect()

    date = filters.map(get_date)
    datereduce = date.aggregateByKey(0, lambda a, b: a + b, lambda a, b: a + b)
    percentDate = datereduce.map(lambda line: (line[0], line[1], (float(line[1])/float(total[0]))*100))
    ordered = percentDate.sortBy(lambda a: a[2], ascending=False)

    result = ordered.collect()
    for item in result:
        print(item)pass

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    station_name = "Fitzgerald's Park"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/3_Assignment/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, station_name)
