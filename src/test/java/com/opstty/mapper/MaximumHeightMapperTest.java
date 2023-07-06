package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.verify;

public class MaximumHeightMapperTest {
    @Mock
    private Mapper<Object, Text, Text, DoubleWritable>.Context context;

    private MaximumHeightMapper maximumHeightMapper;

    @Before
    public void setup() {
        this.context = Mockito.mock(Mapper.Context.class);
        this.maximumHeightMapper = new MaximumHeightMapper();
    }

    @Test
    public void testMapper() throws IOException, InterruptedException {
        LongWritable key = new LongWritable(0);
        Text value = new Text("(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars");
        this.maximumHeightMapper.map(key, value, this.context);
        verify(this.context).write(new Text("Maclura"), new DoubleWritable(13.0));
    }
}
