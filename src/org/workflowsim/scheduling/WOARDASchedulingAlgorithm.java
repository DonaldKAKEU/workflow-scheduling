package org.workflowsim.scheduling;

import java.util.*;
import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.CondorVM;

public class WOARDASchedulingAlgorithm extends BaseSchedulingAlgorithm {

    private int popSize;
    private int taskNum;
    private int vmNum;

    // Population actuelle (chaque ligne est un individu/whale/deer)
    private int[][] population;
    private double[] fitness;

    // Meilleure solution globale
    private int[] gBestSchedule;
    private double gBestFitness = Double.MAX_VALUE;

    private Random rnd = new Random(42);

    // Paramètres RDA
    private double ELITE_RATE = 0.2;

    public WOARDASchedulingAlgorithm(int popSize, int maxIter, int taskNum, int vmNum) {
        this.popSize = popSize;
        this.taskNum = taskNum;
        this.vmNum = vmNum;
        this.population = new int[popSize][taskNum];
        this.fitness = new double[popSize];
        initPopulation();
    }

    // ① Initialisation de la population (Étape (1) de la doc)
    private void initPopulation() {
        for (int i = 0; i < popSize; i++) {
            for (int j = 0; j < taskNum; j++) {
                population[i][j] = rnd.nextInt(vmNum);
            }
            fitness[i] = Double.MAX_VALUE;
        }
    }

    // ② Mise à jour WOA + RDA (Appelée par WorkflowEngine à chaque fin de génération)
    public void updatePopulation(int t, int maxIter) {
        // Trouver le meilleur de la génération actuelle pour WOA
        int currentBestIdx = 0;
        for (int i = 1; i < popSize; i++) {
            if (fitness[i] < fitness[currentBestIdx]) currentBestIdx = i;
        }
        int[] best = population[currentBestIdx];

        // --- PHASE WOA ---
        double a = 2.0 * (1.0 - (double) t / (double) maxIter);
        for (int i = 0; i < popSize; i++) {
            double A = 2 * a * rnd.nextDouble() - a;
            double C = 2 * rnd.nextDouble();
            double p = rnd.nextDouble();

            if (p < 0.5) {
                if (Math.abs(A) < 1) {
                    population[i] = moveTowards(best, population[i], A, C);
                } else {
                    int r = rnd.nextInt(popSize);
                    population[i] = moveTowards(population[r], population[i], A, C);
                }
            } else {
                population[i] = spiralUpdate(population[i], best);
            }
        }

        // --- PHASE RDA (Hybridation locale) ---
        applyRdaLocal(best);

        // Mise à jour du Global Best historique
        updateGlobalBest();
    }

    // Mécanismes RDA (Roaring & Fighting)
    private void applyRdaLocal(int[] best) {
        int eliteCount = Math.max(2, (int) (ELITE_RATE * popSize));
        // On trie par fitness pour identifier l'élite (les mâles dominants)
        Integer[] indices = new Integer[popSize];
        for (int i = 0; i < popSize; i++) indices[i] = i;
        Arrays.sort(indices, (a, b) -> Double.compare(fitness[a], fitness[b]));

        // Roaring & Fighting sur l'élite
        for (int i = 0; i < eliteCount; i++) {
            roaring(population[indices[i]]);
            if (i + 1 < eliteCount) {
                // Petit combat/croisement entre deux élites
                population[indices[i]] = mate(population[indices[i]], population[indices[i+1]]);
            }
        }
    }

    private void updateGlobalBest() {
        for (int i = 0; i < popSize; i++) {
            if (fitness[i] < gBestFitness) {
                gBestFitness = fitness[i];
                gBestSchedule = population[i].clone();
            }
        }
    }

    // Getters pour le WorkflowEngine et le FogBroker (étapes ⑦, ⑧, ⑨)
    public int[] getSchedule(int index) {
        return population[index];
    }

    public int[] getGBestSchedule() {
        return gBestSchedule;
    }

    /**
     * Retourne l'individu actuel au format double[] attendu par FogBroker
     * Équivalent à woa.getWhale()
     */
    public double[] getWhale(int index) {
        if (index < 0 || index >= population.length) return null;

        int[] schedule = population[index];
        double[] dSchedule = new double[schedule.length];
        for (int i = 0; i < schedule.length; i++) {
            dSchedule[i] = (double) schedule[i];
        }
        return dSchedule;
    }

    /**
     * Retourne la meilleure solution globale au format double[]
     * Équivalent à woa.getBestWhale()
     */
    public double[] getBestWhale() {
        if (gBestSchedule == null) return null;

        double[] dSchedule = new double[gBestSchedule.length];
        for (int i = 0; i < gBestSchedule.length; i++) {
            dSchedule[i] = (double) gBestSchedule[i];
        }
        return dSchedule;
    }

    // Retourne la meilleure fitness (score) trouvée depuis le début
    public double getGBestFitness() {
        return this.gBestFitness;
    }

    public void setFitness(int index, double value) {
        this.fitness[index] = value;
    }

    public int getPopSize() { return popSize; }

    // Opérateurs mathématiques WOA
    private int[] moveTowards(int[] target, int[] current, double A, double C) {
        int[] res = new int[taskNum];
        for (int i = 0; i < taskNum; i++) {
            int step = (int) Math.round((C * target[i] - current[i]) * A);
            res[i] = Math.abs(current[i] + step) % vmNum;
        }
        return res;
    }

    private int[] spiralUpdate(int[] current, int[] best) {
        int[] res = new int[taskNum];
        double b = 1.0;
        double l = rnd.nextDouble() * 2 - 1;
        for (int i = 0; i < taskNum; i++) {
            int dist = Math.abs(best[i] - current[i]);
            double val = dist * Math.exp(b * l) * Math.cos(2 * Math.PI * l);
            res[i] = Math.abs(current[i] + (int) Math.round(val)) % vmNum;
        }
        return res;
    }

    private void roaring(int[] e) {
        int pos = rnd.nextInt(taskNum);
        e[pos] = rnd.nextInt(vmNum);
    }

    private int[] mate(int[] a, int[] b) {
        int[] c = new int[taskNum];
        int cut = rnd.nextInt(taskNum);
        for (int i = 0; i < taskNum; i++) c[i] = (i < cut ? a[i] : b[i]);
        return c;
    }

    @Override
    public void run() {
        // La méthode run() reste vide ou minimale car l'exécution
        // est pilotée par les événements du WorkflowEngine (Step ⑥ de la doc)
    }

}