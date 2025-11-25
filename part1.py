"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda kv: f(kv[0], kv[1]))

def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(f) # combines all vals with same key using f, returns new RDD of (k2, v2) w/1 value per key

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
An example of this can be a company tracking sales. They start by mapping sales per country to the key country, 
then they combine all the sales for each country to get total sales in the reduce stage. 
=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

""" 
* ORIGINAL PART 1 FUNCTION
def load_input():
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    return sc.parallelize(range(1, 1000001)) 
"""
# NEW PART 3 FUNCTION
def load_input():
    return sc.parallelize(range(1, 1000001)) # make Spark RDD of ints between 1-1,000,000


def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    return rdd.count() # return length of dataset

"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    # TODO
    mapped = rdd.map(lambda x:(None, x)) # map each # to single key 'None'
    mapped_pairs = general_map(mapped, lambda k, v: [(k, (v, 1))]) # prepare sum count pairs
    reduced = general_reduce(mapped_pairs, lambda a, b: (a[0] + b[0], a[1] + b[1])) # reduce by key to sum vals & counts
    key_value_pair = reduced.collect()[0] # get first element
    total_sum, total_count = key_value_pair[1] # extract (sum, count) tuple
    return total_sum / total_count #calc avg

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    digit_paired = general_map(rdd.map(lambda x: (None, x)), lambda k, v: [(d, 1) for d in str(v)]) # gives RDD of (digit, 1)
    digit_counts = general_reduce(digit_paired, lambda a, b: a + b) # reduce by digit to get freq.
    counts = digit_counts.collect() 
    # find most and least common digits
    most_common = max(counts, key = lambda x: x[1])
    most_common_digit = most_common[0]
    most_common_frequency = most_common[1]
    least_common = min(counts, key = lambda x: x[1])
    least_common_digit = least_common[0]
    least_common_frequency = least_common[1]
    return(most_common_digit, most_common_frequency, least_common_digit, least_common_frequency)


"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# *** Define helper function(s) here ***

# moved the helpers into q7 function to avoid PySparkRuntimeError 
def q7(rdd):
    def num_to_word(n): 
        
        # unique numbers to words
        if n == 0:
            return "zero"
        if n == 1000000:
            return "one million"

        # make each digit place a list to index
        ones = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"] 
        teens = ["ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
                 "sixteen", "seventeen", "eighteen", "nineteen"]
        tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]

        def three_digit_nums(num):
            word = ""
            h = num // 100 #extract hundredths place
            t = num % 100 #extract tenths place 
            
            # assigning h and t to a word from list
            if h > 0: 
                word += ones[h] + " hundred"
                if t > 0:
                    word += " and "
            if 10 <= t <= 19:
                word += teens[t-10]
            else:
                if t >= 20:
                    word += tens[t//10]
                    if t % 10 > 0:
                        word += " " + ones[t % 10]
                elif t > 0:
                    word += ones[t]
            return word.strip()

        # assigning words to thousandths place
        if n >= 1000:
            thousands = n // 1000
            remainder = n % 1000
            if thousands > 999:
                thousands = 999
            result = three_digit_nums(thousands) + " thousand"
            if remainder > 0:
                result += " " + three_digit_nums(remainder)
        else:
            result = three_digit_nums(n)

        return result.strip()

    letters_rdd = general_map(
        rdd.map(lambda x: (None, x)),
        lambda k, v: [(c, 1) for c in num_to_word(v).replace(" ", "").replace("-", "")]
    )
    letter_counts = general_reduce(letters_rdd, lambda a, b: a + b)
    counts = letter_counts.collect()
    most = max(counts, key=lambda x: x[1])
    least = min(counts, key=lambda x: x[1])
    return (most[0], most[1], least[0], least[1])



"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in at most 3 minutes.
- If you are unable to run up to 100 million on your machine within the time
  limit, please change the input to 10 million instead of 100 million.
  If it is still taking too long even for that,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

"""
* ORIGINAL PART 1 FUNC
def load_input_bigger():
    return sc.parallelize(range(1, 10000001), numSlices = 100)
"""
#NEW PART 3 FUNC
def load_input_bigger(N = None, P = None):
    if N is None:
        return sc.parallelize(range(1, 100000001), numSlices=100)
    if P is None:
        return sc.parallelize(range(1, N+1))
    else:
        return sc.parallelize(range(1, N+1), P)

""" 
*ORIGINAL Q8 a and b
def q8_a():
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # rdd = load_input_bigger(N, P)
    rdd = load_input_bigger() 
    return q6(rdd)

def q8_b():
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    #rdd = load_input_bigger(N, P)
    rdd = load_input_bigger()
    return q7(rdd)
"""
# NEW PART 3 Q8 a and b
def q8_a(N = None, P = None):
    rdd = load_input_bigger(N, P)
    return q6(rdd)

def q8_b(N = None, P = None):
    rdd = load_input_bigger(N, P)
    return q7(rdd)


"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
Q6: 
k1 = none (mapped all vals to a single key to flatten)
v1 = the number (integer from RDD)
k2 = the key after flatMap (individual chars in q7, or the same None is still aggregated)
v2= the count for each char

Q7: 
k1 = none (all nums start under single key)
v1 = number value (int)
k2 = char from spelled out number string
v2 =1 (char count)
=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===
No. The simplified version only supports a single map and reduce step with one key value pair at a time. We can compute using 2 steps: converting numbers to words, 
match each char to (char, 1), then reduce by char to get sum counts. Simplified MapReduce doesn't have operations like chaining transformations or handling nested operations in
comparison to flatMap. 
=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    kv_RDD = rdd.map(lambda x: (None, x)) # wrap each element as a tuple
    mapped_RDD = general_map(kv_RDD, lambda k, v : []) # return empty list for each input
    reduced_RDD = general_reduce(mapped_RDD, lambda a, b: a + b) # reduce stage = no output b/c map produced nothing
    return set(reduced_RDD.collect()) #return set of key value pairs



"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
The answer for q11 is: set(). The map stage returned an empty list for every input, resulting in no (key, value) pairs for the next stage, reduce stage, 
to process. This makes the final output an empty set, hence, set(). This is due to general_reduce's capibility of combining values only for keys that exist.
In short, map makes nothing = nothing to reduce. 
=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
The output of the reduce stage is different depending on the order of the input because general_reduce combines the values pair by pair, using the provided function. 
If the function wasn't commutative or associative, the order of the combination of pairs will be different. 
=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    rdd_pairs = rdd.map(lambda x: (None, x)) # map og RDD to (kv, v1) if not, int unpacking error occurs
    mapped_RDD = general_map(rdd_pairs, lambda k, v: [(1, v)]) # mapping k2 =1 , v2 = number
    reduced_RDD = general_reduce(mapped_RDD, lambda a, b: a - b) # reduce by subtraction vals 
    return set(reduced_RDD.collect())

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
Run #1: {(1, 484368374988)}
Run #2: {(1, 484368374988)}      
It does not exhibit nondeterministic behavior on different runs. 
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # RDD with 2 partitions
    data = [(0,10), (1, 20), (3, 30), (4, 40), (5, 50)]
    rdd = sc.parallelize(data, 2)
    mapped_RDD = general_map(rdd, lambda k, v: [(1, v)])
    reduced_RDD = general_reduce(mapped_RDD, lambda a, b: a - b)
    return set(reduced_RDD.collect())

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # RDD with 4 partitions
    data = [(0,10), (1, 20), (3, 30), (4, 40), (5, 50)]
    rdd = sc.parallelize(data, 4)
    mapped_RDD = general_map(rdd, lambda k, v: [(1, v)])
    reduced_RDD = general_reduce(mapped_RDD, lambda a, b: a - b)
    return set(reduced_RDD.collect())

def q16_c():
        #RDD with 6 partitions
        data = [(0,10), (1, 20), (3, 30), (4, 40), (5, 50)]
        rdd = sc.parallelize(data, 6)
        mapped_RDD = general_map(rdd, lambda k, v: [(1, v)])
        reduced_RDD = general_reduce(mapped_RDD, lambda a, b: a - b)
        return set(reduced_RDD.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
Yes. 
q16a answer: {(1, 50)} 
q16b answer: {(1, -30)} 
q16c answer: {(1, -130)}
=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
Yes. If the pipeline uses non-associative/communitative reduce functions, the final output may differ. This means that the same input in the same job can produce different answers.
=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
"More importantly, our investigation indicates that most of those non-
commutative reducers do not lead to correctness issues."

I found this interesting because although the study revealed that more than 50% of reducers were non-commutative, there were no issues. 
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

#I chose the ex: Last-Value Reducer Pattern

def q20():
    
    data = [("row1", 10), ("row2", 20), ("row3", 30)] # create RDD (cannot put as arg)
    rdd = sc.parallelize(data, 3)
    mapped_RDD = general_map(rdd, lambda k, v: [(1, v)])
    reduced_RDD = general_reduce(mapped_RDD, lambda old, new: new) # overwrting old val
    reduced_RDD.collect()
    return True


"""
That's it for Part 1!

===== Wrapping things up =====

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

    # Create SparkSession and SparkContext **only when running this module directly**
    spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
    sc = spark.sparkContext

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])


    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
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

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
