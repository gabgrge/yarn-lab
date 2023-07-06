package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistrictsContainingTreesReducerTest {
    @Mock
    private Reducer.Context context;

    private DistrictsContainingTreesReducer reducer;

    @Before
    public void setup() {
        this.reducer = new DistrictsContainingTreesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(7);
        Iterable<NullWritable> values = Arrays.asList(NullWritable.get(), NullWritable.get(), NullWritable.get());

        this.reducer.reduce(key, values, this.context);

        verify(this.context).write(key, NullWritable.get());
    }
}
