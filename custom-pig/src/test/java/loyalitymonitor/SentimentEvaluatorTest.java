package loyalitymonitor;

import junit.framework.Assert;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentEvaluatorTest {
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    private SentimentsEvaluator sentimentsEvaluator;

    @Before
    public void setUp() {
        Map<String, Integer> dic = new HashMap<String, Integer>();
        dic.put("great", 5);
        dic.put("bad", -2);
        dic.put("amazing", 10);
        dic.put("pity", -5);

        this.sentimentsEvaluator = new SentimentsEvaluator();
        this.sentimentsEvaluator.setSentiments(dic);
    }

    @Test
    public void testSentimentScore() throws IOException {
        Tuple tuple = tupleFactory.newTuple();
        tuple.append("Yesterday was increddible, amazing, great sunny day!");
        Tuple t = sentimentsEvaluator.exec(tuple);
        Assert.assertEquals( "Expected tuple size is 1", 1, t.size() );
        Assert.assertEquals( "Expected sentiment score is 15", 15, t.get(0) );
    }

}