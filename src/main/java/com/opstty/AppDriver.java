package com.opstty;

import com.opstty.job.TreeKindCountJob;
import com.opstty.job.WordCount;
import com.opstty.job.SpeciesJob;
import com.opstty.job.DistrictsContainingTrees;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("districtscontainingtrees", DistrictsContainingTrees.class,
                    "A map/reduce program that displays the list of districts containing trees.");

            programDriver.addClass("species", SpeciesJob.class,
                    "A map/reduce program that displays the list of different species trees.");

            programDriver.addClass("treekind", TreeKindCountJob.class,
                    "A map/reduce program that calculates the number of trees of each kind.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
