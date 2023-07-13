package com.opstty;

import com.opstty.job.*;
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

            programDriver.addClass("maximumheight", MaximumHeightJob.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind.");

            programDriver.addClass("sortheight", TreeHeightSortJob.class,
                    "A map/reduce program that sort the trees height from smallest to largest.");

            programDriver.addClass("oldesttree", OldestTreeJob.class,
                    "A map/reduce program that displays the district where the oldest tree is.");

            programDriver.addClass("treecount", TreeCountJob.class,
                    "A map/reduce program that displays the district that contains the most trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
