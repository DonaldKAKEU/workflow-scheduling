package org.workflowsim.scheduling;

import java.util.Random;

/**
 * WOA core: each whale = a full schedule (task -> vm).
 * Representation: whale[t] = vmId
 */
public class WoaScheduling {

    private final int populationSize;
    private final int maxIterations;
    private final int taskNum;
    private final int vmNum;

    private final double[][] whales;   // [whale][task] position (will be discretized)
    private final double[] fitness;    // fitness of each whale
    private final double[] bestWhale;  // global best schedule
    private double bestFitness;

    private final Random rand = new Random();

    public WoaScheduling(int populationSize, int maxIterations, int taskNum, int vmNum) {
        if (taskNum <= 0) throw new IllegalArgumentException("taskNum must be > 0");
        if (vmNum <= 0) throw new IllegalArgumentException("vmNum must be > 0");

        this.populationSize = populationSize;
        this.maxIterations = maxIterations;
        this.taskNum = taskNum;
        this.vmNum = vmNum;

        this.whales = new double[populationSize][taskNum];
        this.fitness = new double[populationSize];
        this.bestWhale = new double[taskNum];
        this.bestFitness = Double.MAX_VALUE;

        initPopulation();
    }

    private void initPopulation() {
        for (int i = 0; i < populationSize; i++) {
            for (int t = 0; t < taskNum; t++) {
                whales[i][t] = rand.nextInt(vmNum); // already discrete
            }
            fitness[i] = Double.MAX_VALUE;
        }
        // initialize bestWhale as whale 0
        System.arraycopy(whales[0], 0, bestWhale, 0, taskNum);
    }

    public int getPopulationSize() {
        return populationSize;
    }

    public int getTaskNum() {
        return taskNum;
    }

    public int getVmNum() {
        return vmNum;
    }

    public double[] getWhale(int index) {
        return whales[index];
    }

    public double[] getBestWhale() {
        return bestWhale;
    }

    public double getBestFitness() {
        return bestFitness;
    }

    public void setFitness(int whaleIndex, double value) {
        fitness[whaleIndex] = value;
        if (value < bestFitness) {
            bestFitness = value;
            System.arraycopy(whales[whaleIndex], 0, bestWhale, 0, taskNum);
        }
    }

    /**
     * Update whales using WOA equations + discretization
     */
    public void updateWhales(int iteration) {
        // a decreases linearly from 2 to 0
        double a = 2.0 - iteration * (2.0 / (double) maxIterations);

        for (int i = 0; i < populationSize; i++) {

            double r1 = rand.nextDouble();
            double r2 = rand.nextDouble();
            double A = 2.0 * a * r1 - a;
            double C = 2.0 * r2;

            double p = rand.nextDouble();

            for (int t = 0; t < taskNum; t++) {

                double newPos;

                if (p < 0.5) {
                    if (Math.abs(A) < 1.0) {
                        // encircling the best whale
                        double D = Math.abs(C * bestWhale[t] - whales[i][t]);
                        newPos = bestWhale[t] - A * D;
                    } else {
                        // search around a random whale
                        int randIdx = rand.nextInt(populationSize);
                        double D = Math.abs(C * whales[randIdx][t] - whales[i][t]);
                        newPos = whales[randIdx][t] - A * D;
                    }
                } else {
                    // spiral
                    double b = 1.0;
                    double l = rand.nextDouble() * 2.0 - 1.0; // [-1,1]
                    double D = Math.abs(bestWhale[t] - whales[i][t]);
                    newPos = D * Math.exp(b * l) * Math.cos(2.0 * Math.PI * l) + bestWhale[t];
                }

                // discretize to vmId
                whales[i][t] = discretizeVm(newPos);
            }
        }
    }

    private double discretizeVm(double x) {
        long v = Math.round(x);
        if (v < 0) v = -v;
        return (double) (v % vmNum);
    }
}
