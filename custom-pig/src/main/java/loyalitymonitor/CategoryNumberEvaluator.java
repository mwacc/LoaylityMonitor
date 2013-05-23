package loyalitymonitor;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CategoryNumberEvaluator extends EvalFunc<DataBag> {
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private static final BagFactory bagFactory = BagFactory.getInstance();

    private Multimap<String, String> categoryDims = null;

    private void loadCategoriesMap() throws IOException {
        Multimap<String, String> categoryDimsMap =  LinkedListMultimap.create();

        // 1 -> Obama, US president
        BufferedReader in = new BufferedReader(new FileReader("categories"));
        String categ = null;
        while ((categ = in.readLine()) != null) {
            String[] map = categ.split("\t");
            for(String s: map[1].split(",")) {
                // put Obama -> 1
                categoryDimsMap.put(s, map[0]);
            }
        }

        this.categoryDims = categoryDimsMap;
    }

    public void setCategoryDims(LinkedListMultimap dictionary) {
        this.categoryDims = dictionary;
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if( categoryDims == null ) {
            loadCategoriesMap();
        }
        if (input == null || input.size() == 0) {
            return null;
        }
        DataBag outputBag = bagFactory.newDefaultBag();

        String inputLine = (String)input.get(0);
        String[] splitInputLine = inputLine.split("\\s");
        Set<String> campaignIds = new HashSet<String>();
        for(String s : splitInputLine) {
            // remove all punctuation symbols
            s = s.replaceAll("(\\w+)\\p{Punct}(\\s|$)", "$1$2");
            if( categoryDims.containsKey(s) ) {
                for( String categoryId : categoryDims.get(s) ) {
                    if( campaignIds.contains(categoryId) ) continue;

                    addTuple(outputBag, categoryId, inputLine);
                    campaignIds.add(categoryId);
                }
            }
        }

        return outputBag;
    }


    protected void addTuple(DataBag outputBag, String message, String categoryId) throws ExecException {
        Tuple outputTuple = tupleFactory.newTuple();
        outputTuple.append(message);
        outputTuple.append(categoryId);
        outputBag.add(outputTuple);
    }
}