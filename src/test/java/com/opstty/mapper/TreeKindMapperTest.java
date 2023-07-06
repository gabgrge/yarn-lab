package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.mockito.Mockito.verify;

public class TreeKindMapperTest {
    @Mock
    private Mapper.Context context;

    private TreeKindMapper treeKindMapper;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.treeKindMapper = new TreeKindMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        LongWritable key = new LongWritable(1);
        Text value = new Text("(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars");
        this.treeKindMapper.map(key, value, this.context);
        verify(this.context).write(new Text("Maclura"), new IntWritable(1));
    }
}
