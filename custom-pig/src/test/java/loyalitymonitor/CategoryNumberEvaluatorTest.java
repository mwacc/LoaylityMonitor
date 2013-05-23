package loyalitymonitor;


import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import junit.framework.Assert;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CategoryNumberEvaluatorTest {

    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private CategoryNumberEvaluator evaluator;

    @Before
    public void setUp() {
        LinkedListMultimap<String, String> dic =  LinkedListMultimap.create();
        dic.put("Obama", "1");
        dic.put("US President","1");
        dic.put("Intel","2");

        evaluator = new CategoryNumberEvaluator();
        evaluator.setCategoryDims(dic);
    }

    @Test
    public void testOneBrandInTweet() throws IOException {
        Tuple tuple = tupleFactory.newTuple();
        tuple.append("Obama decided to go to vacation in August");

        DataBag bag = evaluator.exec(tuple);
        Assert.assertEquals("More than one tuple has been emitted by evaluator", 1, bag.size());
    }

    @Test
    public void testOneBrandDuplicateInTweet() throws IOException {
        Tuple tuple = tupleFactory.newTuple();
        tuple.append("Barak Obama decided to cancel vacation for US President");

        DataBag bag = evaluator.exec(tuple);
        Assert.assertEquals("More than one tuple has been emitted by evaluator", 1, bag.size());
    }

    @Test
    public void testTwoBrandInTweet() throws IOException {
        Tuple tuple = tupleFactory.newTuple();
        tuple.append("Obama has had lunch at Intel office");

        DataBag bag = evaluator.exec(tuple);
        Assert.assertEquals("Two tuples have been expected to be emitted by evaluator", 2, bag.size());
    }

}