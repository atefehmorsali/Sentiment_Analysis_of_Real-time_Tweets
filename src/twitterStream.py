
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    print counts
    make_plot(counts)

#Plot the counts for the positive and negative words for each timestep.
def make_plot(counts):
  
    poslist=[]
    neglist=[]
      
    for i in range(len(counts)):
        if len(counts[i])>0:
            poslist.append(counts[i][0][1])
            neglist.append(counts[i][1][1])
    xaxism=range(len(poslist))
    plt.plot(xaxism,poslist,'bs-',label="positive")
    plt.plot(xaxism,neglist,'gs-',label="negative")
    plt.legend(loc='upperleft')
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.show()
    print poslist

    

def load_wordlist(filename):
    #returns a list or set of words 
    listw=[]
    f=open(filename,'r')
    for line in f:
        line=line.rstrip()
        listw.append(line)
    return listw   


 
def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    
   
    # Each element of tweets is the text of a tweet
    # We need to find the count of all the positive and negative words in these tweets
    # Keep track of a running total counts and print this at every time step using the pprint function
    
  
    words=tweets.flatMap(lambda line: line.split(" "))
    
    temp_pairs=words.map(lambda t: ("positive") if t in pwords  else ("negative") if t in nwords else (""))
   
    pairs1=temp_pairs.filter(lambda x: (x=="positive" or x=="negative"))
    pairs = pairs1.map(lambda x: (x,1))	
    wordCounts_temp=pairs.reduceByKey(lambda x,y: x+y)
    wordCounts=wordCounts_temp.updateStateByKey(updateFunction)
    wordCounts.pprint()
  
    
    # the counts variable hold the word counts for all time steps  
    counts = []
    wordCounts_temp.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    # counts: [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    return counts


def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount=0
    return sum(newValues,runningCount)


if __name__=="__main__":
    main()
