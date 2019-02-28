package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        // Compute sum of reciprocals of array elements
        long startTime = System.nanoTime();
        double sum = seqArraySum(input, 0, input.length);
        long endTime = System.nanoTime();
        printResults("seqArrSum", (endTime - startTime), sum);
        return sum;
    }

    /**
     * Utility method added by me to so it can also be used in ReciprocalArraySumTask
     *
     * @param input
     * @param startIndexInclusive
     * @param endIndexExclusive
     * @return
     */
    private static double seqArraySum(double[] input, int startIndexInclusive, int endIndexExclusive) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks   The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the start of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     * nElements
     */
    private static int getChunkStartInclusive(final int chunk,
                                              final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the end of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
                                            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {

        private static final int THRESHOLD = 100000;
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;
        /**
         * custom variable to hold the number of sub tasks this task should split into;
         */
        private int numTasks;
        /**
         * custom variable to hold whether the computation has to be done sequentially.
         */
        private boolean seq;

        /**
         * Constructor.
         *
         * @param setStartIndexInclusive Set the starting index to begin
         *                               parallel traversal at.
         * @param setEndIndexExclusive   Set ending index for parallel traversal.
         * @param setInput               Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive, final double[] setInput) {
            this(setStartIndexInclusive, setEndIndexExclusive, setInput, 2);
        }

        /**
         * Constructor.
         *
         * @param setStartIndexInclusive Set the starting index to begin
         *                               parallel traversal at.
         * @param setEndIndexExclusive   Set ending index for parallel traversal.
         * @param setInput               Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive, final double[] setInput, int setNumTasks) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.numTasks = setNumTasks;
//            System.out.printf("startIndexInclusive: %d, endIndexExclusive: %d, numTasks: %d \n", startIndexInclusive, endIndexExclusive, numTasks);
        }

        /**
         * Getter for the value produced by this task.
         *
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            if (startIndexInclusive == endIndexExclusive - 1) {
                value = input[startIndexInclusive];
//                System.out.println("case1");
            }
            else if (startIndexInclusive >= endIndexExclusive) {
                value = 0;
//                System.out.println("case2");
            }
            else if (endIndexExclusive - 1 - startIndexInclusive <= THRESHOLD) {
                value = seqArraySum(input, startIndexInclusive, endIndexExclusive);
//                System.out.println("case3");
            }
            else {
//                int mid = (startIndexInclusive + endIndexExclusive) / 2;
//                ReciprocalArraySumTask left = new ReciprocalArraySumTask(startIndexInclusive, mid, input);
//                ReciprocalArraySumTask right = new ReciprocalArraySumTask(mid, endIndexExclusive, input);
//                left.fork();
//                right.compute();
//                left.join();
//                value = left.value + right.value;
                List<ReciprocalArraySumTask> subTasks = getSubTasks();
                invokeAll(subTasks);
//                                .stream()
//                                .mapToDouble(task -> task.value)
//                                .sum();

                for(ReciprocalArraySumTask subTask : subTasks) {
                    value += subTask.value;
                }

            }
        }

        protected List<ReciprocalArraySumTask> getSubTasks() {
            List<ReciprocalArraySumTask> tasks = new ArrayList<>();
            for (int i = 0; i < numTasks; i++) {
                int _startIndexInclusive = startIndexInclusive + getChunkStartInclusive(i, numTasks, endIndexExclusive - startIndexInclusive);
                int _endIndexExclusive = startIndexInclusive + getChunkEndExclusive(i, numTasks, endIndexExclusive - startIndexInclusive);
//                System.out.println("_startIndexInclusive: " + _startIndexInclusive + ", _endIndexExclusive: " + _endIndexExclusive);
                ReciprocalArraySumTask task = new ReciprocalArraySumTask(_startIndexInclusive, _endIndexExclusive, input, numTasks);
                tasks.add(task);
            }
//            System.out.println("tasks size: " + tasks.size());
            return tasks;
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;
        return parManyTaskArraySum(input, 2);

    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input    Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
                                                final int numTasks) {

        ForkJoinPool pool = new ForkJoinPool();
        long startTime = System.nanoTime();
        ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length, input, numTasks);
        pool.invoke(task);
        long endTime = System.nanoTime();
        printResults("parArrSum", (endTime - startTime), task.value);
        return task.value;

    }

    private static void printResults(String name, long timeInNanos, double sum) {
        System.out.printf("  %s completed in %.3f milliseconds, with sum = %.5f \n", name, timeInNanos / 1e6, sum);
    }


    public static void main(String[] args) {
        int n = 100000000;
        double[] a = new double[n];
        for (int i = 0; i < n; i++) {
            a[i] = i + 1; // i + 1 because we have to avoid idv by zero error.
        }
        //System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");

        for (int i = 0; i < 5; i++) {
            System.out.println("Run " + i);
            seqArraySum(a);
            parArraySum(a);
        }
    }
}
