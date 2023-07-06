package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

public class MaximumHeightReducerTest {
    @Mock
    private Reducer.Context context;

    private MaximumHeightReducer maximumHeightReducer;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.maximumHeightReducer = new MaximumHeightReducer();
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {
        Text key = new Text("Maclura");
        Iterable<DoubleWritable> values = Arrays.asList(
                new DoubleWritable(13.0),
                new DoubleWritable(11.0),
                new DoubleWritable(15.0)
        );
        this.maximumHeightReducer.reduce(key, values, this.context);
        verify(this.context).write(key, new DoubleWritable(15.0));
    }
}
