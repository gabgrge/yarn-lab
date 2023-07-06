package com.opstty.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

public class SpeciesReducerTest {
    @Mock
    private Reducer.Context context;
    private SpeciesReducer speciesReducer;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.speciesReducer = new SpeciesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Text key = new Text("Maclura");
        Iterable<Text> values = Arrays.asList(new Text(), new Text(), new Text());
        this.speciesReducer.reduce(key, values, this.context);
        verify(this.context).write(key, new Text());
    }
}
