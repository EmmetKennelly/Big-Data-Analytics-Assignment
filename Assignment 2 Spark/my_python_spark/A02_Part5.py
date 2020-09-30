# This is just code used to test out the map reduce with made up data


# import sys
# import codecs
# from functools import reduce

# file = """
# Wikipedia ; Roger Federer ; French ; 23 
# WikiBooks ; Sapiens ; English ; 8 
# Wikipedia ; Taoiseach ; English ; 10 
# Wikipedia ; Olympic Games ; English ; 211 
# Wikipedia ; Musee du Louvre ; French ; 15 
# WikiVoyage ; Hawai ; English ; 5 
# Wikipedia ; Ronald Garros ; French ; 119 
# WikiBooks ; Pinocchio ; Italian ; 7 
# """


# def process(my_input_stream):
#     lines = my_input_stream.split("\n")
  #   noEmptyLines = list(filter(lambda line: len(line) > 0, lines))
 #    return map(lambda d: my_map(d), noEmptyLines)


# def my_map(line):
#     my_tuple = line.split(" ; ")
#     res = {
#         'project': my_tuple[0],
#         'web_name': my_tuple[1],
#         'language': my_tuple[2],
#         'views': int(my_tuple[3])
 #    }
 #    return res

 # def getViews(line):
 #     return line['views']


# line1 is
 # def my_reduce(key, line):
 #     projectAndLang = line['project'] + "_" + line['language'] 
 #     if key is not None and projectAndLang not in key[0]:
 #         key[0] = [*key[0], projectAndLang]
  #  #        key[1] = [*key[1], "(" + line["web_name"]+ ", "+ str(line['views'])+")" ]
  #    return accumulated

 # sortedByViews = sorted(list(process(file)), key=getViews, reverse=True)
 # test = reduce(my_reduce, sortedByViews, [[], []])
 # for i in range(0, len(test[0])):
 #     print(test[0][i] + "       " + test[1][i])


  
# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------ 
  
  
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
  
    for line in my_input_stream:
        my_tuple = process_line(line)

        res = {
        'project': my_tuple[0],
        'web_name': my_tuple[1],
        'language': my_tuple[2],
        'views': int(my_tuple[3])
         }
    return res


def getViews(line):
    return line['views']

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters,key,line):
   projectAndLang = line['project'] + "_" + line['language'] 
    if key is not None and projectAndLang not in key[0]:
        key[0] = [*key[0], projectAndLang]
        key[1] = [*key[1], "(" + line["web_name"]+ ", "+ str(line['views'])+")" ]
    return accumulated

sortedByViews = sorted(list(process(my_input_stream)), key=getViews, reverse=True)
strs = reduce(my_reduce, sortedByViews, [[], []])
for i in range(0, len(strs[0])):
    my_output_stream.write(strs[0][i] + "       " + strs[1][i])

# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):
  input_rdd = sc.textFile(my_dataset_dir)
  my_tuplerdd = input_rdd.map(process_line)
  filters = my_tuple.filter(lambda line: line[0] and line[2])
  projectAndLang = line['project'] + "_" + line['language'] 
  if key is not None and projectAndLang not in key[0]:
    sorted_rdd = sorted(list(process(my_input_stream)), key=line[3], reverse=True)
    ordered = sorted_rdd.sortBy(lambda a: a[3], ascending=True)
    ordered.persist()
    endVal = ordered.collect()
    endVal.aveAsTextFile(my_dataset_dir)

# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
  inputDStream = ssc.textFileStream(monitoring_dir)
  my_tupleStream = input_Stream.map(process_line)    
  allWordsDStream = inputDStream.flatMap(process_line)
  lines = inputStream.reduceByKeyAndWindow(lambda rdd: rdd)
  
  
  filter_Stream = my_tupleStream.filter(lambda line: line[0] and line[2] )
  short_Stream = filter_Stream.map(lambda line: (line[1], 1))
  reduced_Stream = short_Stream.reduceByKey(lambda a, b: a + b)
  projectAndLang = line['project'] + "_" + line['language'] 
  if key is not None and projectAndLang not in key[0]:
  ordered_Stream = reduced_Stream.transform(lambda newValue: newValue.sortBy(lambda a: a[3], ascending=True))
  ordered_Stream.print()
