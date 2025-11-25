"""
Part 3: Measuring Performance

Now that you have drawn the dataflow graph in part 2,
this part will explore how performance of real pipelines
can differ from the theoretical model of dataflow graphs.

We will measure the performance of your pipeline
using your ThroughputHelper and LatencyHelper from HW1.

=== Coding part 1: making the input size and partitioning configurable ===

We would like to measure the throughput and latency of your PART1_PIPELINE,
but first, we need to make it configurable in:
(i) the input size
(ii) the level of parallelism.

Currently, your pipeline should have two inputs, load_input() and load_input_bigger().
You will need to change part1 by making the following additions:

- Make load_input and load_input_bigger take arguments that can be None, like this:

    def load_input(N=None, P=None)

    def load_input_bigger(N=None, P=None)

You will also need to do the same thing to q8_a and q8_b:

    def q8_a(N=None, P=None)

    def q8_b(N=None, P=None)

Here, the argument N = None is an optional parameter that, if specified, gives the size of the input
to be considered, and P = None is an optional parameter that, if specifed, gives the level of parallelism
(number of partitions) in the RDD.

You will need to make both functions work with the new signatures.
Be careful to check that the above changes should preserve the existing functionality of part1
(so python3 part1.py should still give the same output as before!)

Don't make any other changes to the function sigatures.

Once this is done, define a *new* version of the PART_1_PIPELINE, below,
that takes as input the parameters N and P.
(This time, you don't have to consider the None case.)
You should not modify the existing PART_1_PIPELINE.

You may either delete the parts of the code that save the output file, or change these to a different output file like part1-answers-temp.txt.
"""
from part1 import *
import timeit
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

ANSWER_FILE = "output/part1-answers-temp.txt"

def PART_1_PIPELINE_PARAMETRIC(N, P):
    """
    TODO: Follow the same logic as PART_1_PIPELINE
    N = number of inputs
    P = parallelism (number of partitions)
    (You can copy the code here), but make the following changes:
    - load_input should use an input of size N.
    - load_input_bigger (including q8_a and q8_b) should use an input of size N.
    - both of these should return an RDD with level of parallelism P (number of partitions = P).
    """ 
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input(N,P)
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([], P or 1)

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a, N, P)
    log_answer("q8b", q8_b, N, P)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    return "done"

"""
=== Coding part 2: measuring the throughput and latency ===

Now we are ready to measure the throughput and latency.

To start, copy the code for ThroughputHelper and LatencyHelper from HW1 into this file.

Then, please measure the performance of PART1_PIPELINE as a whole
using five levels of parallelism:
- parallelism 1
- parallelism 2
- parallelism 4
- parallelism 8
- parallelism 16

For each level of parallelism, you should measure the throughput and latency as the number of input
items increases, using the following input sizes:
- N = 1, 10, 100, 1000, 10_000, 100_000, 1_000_000.

- Note that the larger sizes may take a while to run (for example, up to 30 minutes). You can try with smaller sizes to test your code first.

You can generate any plots you like (for example, a bar chart or an x-y plot on a log scale,)
but store them in the following 10 files,
where the file name corresponds to the level of parallelism:

output/part3-throughput-1.png
output/part3-throughput-2.png
output/part3-throughput-4.png
output/part3-throughput-8.png
output/part3-throughput-16.png
output/part3-latency-1.png
output/part3-latency-2.png
output/part3-latency-4.png
output/part3-latency-8.png
output/part3-latency-16.png

Clarifying notes:

- To control the level of parallelism, use the N, P parameters in your PART_1_PIPELINE_PARAMETRIC above.

- Make sure you sanity check the output to see if it matches what you would expect! The pipeline should run slower
  for larger input sizes N (in general) and for fewer number of partitions P (in general).

- For throughput, the "number of input items" should be 2 * N -- that is, N input items for load_input, and N for load_input_bigger.

- For latency, please measure the performance of the code on the entire input dataset
(rather than a single input row as we did on HW1).
MapReduce is batch processing, so all input rows are processed as a batch
and the latency of any individual input row is the same as the latency of the entire dataset.
That is why we are assuming the latency will just be the running time of the entire dataset.

- Please set `NUM_RUNS` to `1` if you haven't already. Note that this will make the values for low numbers (like `N=1`, `N=10`, and `N=100`) vary quite unpredictably.
"""
NUM_RUNS = 1

# Copy in ThroughputHelper and LatencyHelper
import time
import matplotlib.pyplot as plt

class ThroughputHelper:
    def __init__(self):
        self.pipelines = []  # list of functions
        self.names = []      # pipeline names
        self.sizes = []      # total input items
        self.throughputs = None

    def add_pipeline(self, name, size, func):
        self.names.append(name)
        self.sizes.append(size)
        self.pipelines.append(func)

    def compare_throughput(self):
        """Measure throughput in items/sec using actual runtime."""
        self.throughputs = []
        for func, num_items, name in zip(self.pipelines, self.sizes, self.names):
            start = time.time()
            func()
            end = time.time()
            throughput = num_items / (end - start)
            print(f"Throughput for N={name}: {int(throughput)} items/sec")
            self.throughputs.append(throughput)
        return self.throughputs

    def generate_plot(self, filename):
        plt.figure()
        plt.bar(self.names, self.throughputs, color="blue")
        plt.ylabel("Throughput (items/sec)")
        plt.title("Pipeline Throughput Comparison")
        plt.xticks(rotation=45)
        plt.savefig(filename)
        plt.close()


class LatencyHelper:
    def __init__(self):
        self.pipelines = []  # list of functions
        self.names = []      # pipeline names
        self.latencies = None

    def add_pipeline(self, name, func):
        self.names.append(name)
        self.pipelines.append(func)

    def compare_latency(self):
        """Measure latency in milliseconds using actual runtime."""
        self.latencies = []
        for func, name in zip(self.pipelines, self.names):
            start = time.time()
            func()
            end = time.time()
            latency_ms = (end - start) * 1000
            print(f"Latency for N={name}: {latency_ms:.2f} ms")
            self.latencies.append(latency_ms)
        return self.latencies

    def generate_plot(self, filename):
        plt.figure()
        plt.bar(self.names, self.latencies, color="blue")
        plt.ylabel("Latency (ms)")
        plt.title("Pipeline Latency Comparison")
        plt.xticks(rotation=45)
        plt.savefig(filename)
        plt.close() 

# Insert code to generate plots here as needed

"""
=== Reflection part ===

Once this is done, write a reflection and save it in
a text file, output/part3-reflection.txt.

I would like you to think about and answer the following questions:

1. What would we expect from the throughput and latency
of the pipeline, given only the dataflow graph?

Use the information we have seen in class. In particular,
how should throughput and latency change when we double the amount of parallelism?

Please ignore pipeline and task parallelism for this question.
The parallelism we care about here is data parallelism.

2. In practice, does the expectation from question 1
match the performance you see on the actual measurements? Why or why not?

State specific numbers! What was the expected throughput and what was the observed?
What was the expected latency and what was the observed?

3. Finally, use your answers to Q1-Q2 to form a conjecture
as to what differences there are (large or small) between
the theoretical model and the actual runtime.
Name some overheads that might be present in the pipeline
that are not accounted for by our theoretical model of
dataflow graphs that could affect performance.

You should list an explicit conjecture in your reflection, like this:

    Conjecture: I conjecture that ....

You may have different conjectures for different parallelism cases.
For example, for the parallelism=4 case vs the parallelism=16 case,
if you believe that different overheads are relevant for these different scenarios.

=== Grading notes ===

- Don't forget to fill out the entrypoint below before submitting your code!
Running python3 part3.py should work and should re-generate all of your plots in output/.

- You should modify the code for `part1.py` directly. Make sure that your `python3 part1.py` still runs and gets the same output as before!

- Your larger cases may take a while to run, but they should not take any
  longer than 30 minutes (half an hour).
  You should be including only up to N=1_000_000 in the list above,
  make sure you aren't running the N=10_000_000 case.

- In the reflection, please write at least a paragraph for each question. (5 sentences each)

- Please include specific numbers in your reflection (particularly for Q2).

=== Entrypoint ===
"""

if __name__ == '__main__':

    PARALLELISM_VALUES = [1, 2, 4, 8, 16]
    SIZES = [1, 10, 100, 1000, 10_000, 100_000, 1_000_000]

    def make_driver_pipeline(N, P):
        def driver_func():
            PART_1_PIPELINE_PARAMETRIC(N, P)
        return driver_func

    for P in PARALLELISM_VALUES:
        # Throughput
        th = ThroughputHelper()
        for N in SIZES:
            th.add_pipeline(
                name=str(N),
                size=2*N,
                func=make_driver_pipeline(N, P)
            )
        th.compare_throughput()
        th.generate_plot(f"output/part3-throughput-{P}.png")

        # Latency
        lh = LatencyHelper()
        for N in SIZES:
            lh.add_pipeline(
                name=str(N),
                func=make_driver_pipeline(N, P)
            )
        lh.compare_latency()
        lh.generate_plot(f"output/part3-latency-{P}.png")



