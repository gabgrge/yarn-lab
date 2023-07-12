package com.opstty.reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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
public class TreeHeightReducerTest {
    @Mock
    private Reducer.Context context;
    private TreeHeightReducer treeHeightReducer;

    @Before
    public void setup() {
        this.treeHeightReducer = new TreeHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Text key = new Text("13.0");
        Text value1 = new Text("(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars");
        Text value2 = new Text("(48.8685686134, 2.31331809304);8;Calocedrus;decurrens;Cupressaceae;1854;20.0;195.0;Cours-la-Reine, avenue Franklin-D.-Roosevelt, avenue Matignon, avenue Gabriel;Cèdre à encens;;11;Jardin des Champs Elysées");
        Iterable<Text> values = Arrays.asList(value1, value2);
        this.treeHeightReducer.reduce(new DoubleWritable(13.0), values, this.context);
        verify(this.context).write(value1, new DoubleWritable(13.0));
        verify(this.context).write(value2, new DoubleWritable(13.0));
    }
}
